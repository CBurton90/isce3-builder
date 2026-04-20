"""
NISAR InSAR Snakemake Pipeline
"""

import json
from pathlib import Path

TRACK = 94 # hardcoded global vars, make dynamic at some point
FRAME = 160
IMAGE = "isce3cuda.sif"

root = Path.cwd()
state_output_dir = root / "state_files" # where we store json state files
rslc_output_dir = root / "inputs/rslc" # where we store NISAR RSLC HDF5 scenes
pairs_path = state_output_dir / f"nisar_rslc_ifg_pairs_track{TRACK}_frame{FRAME}.json"

# load rslc pair state
with open(pairs_path) as f:
    rslc_pairs = json.load(f)

pairs = rslc_pairs["pairs"]

rslc_url_set = {} # all unique rslc scene names/urls
for pair in pairs.values():
    rslc_url_set[pair["ref_rslc"]] = pair["ref_url"]
    rslc_url_set[pair["sec_rslc"]] = pair["sec_url"]

# build a dict that contains the references rslc and then all the pairs related to that reference rslc, this powers windowed download/deletion of data
ref_rslc2pairs = {}
for pair_id, pair in pairs.items():
    rslc_scene_id = pair["ref_rslc"]
    ref_rslc2pairs.setdefault(rslc_scene_id, []).append(pair_id)

def h5_path(scene):
    return str(rslc_output_dir / f"{scene}.h5")

def done_path(pair):
    return str(state_output_dir / f"{pair}_nisar.done")

localrules: make_runconfig, delete_rslc

# check for RSLC hdf5 reference & secondary pair for IFG processing, check YAML config exists for that pair, run processing on cuda device via Apptainer, create .done state file
rule process_pair:
  input:
    ref_rslc_h5 = lambda wildcards: h5_path(pairs[wildcards.pair_id]["ref_rslc"]),
    sec_rslc_h5 = lambda wildcards: h5_path(pairs[wildcards.pair_id]["sec_rslc"]),
    cfg = "configs/{pair_id}_nisar_cuda.yaml"
  output:
    done = done_path("{pair_id}")
  resources:
    mem_mb=100000,
    cpus_per_task=32,
    gres="gpu:3g.40gb:1",
    runtime=120,
  shell:
    """
    ml apptainer

    apptainer exec --nv \
      --pwd /opt \
      --writable-tmpfs \
      --bind ./inputs:/opt/inputs \
      --bind ./outputs:/opt/outputs \
      --bind ./products:/opt/products \
      --bind ./dem:/opt/dem \
      --bind ./configs:/opt/configs \
      --bind ./qa:/opt/qa \
      --bind ./logs:/opt/logs \
      --bind ./patches/geocode_insar.py:/opt/isce3/packages/nisar/workflows/geocode_insar.py \
      {IMAGE} \
      python3 -u /opt/isce3/packages/nisar/workflows/insar.py \
      /opt/configs/{wildcards.pair_id}_nisar_cuda.yaml \
      > logs/isce3_nisar_log_{wildcards.pair_id}.txt 2>&1

    touch {output.done}
    """

# build specific YAML config to process an RSLC pair
rule make_runconfig:
  input:
    template = "configs/insar-cuda-template.yaml"
  output:
    cfg = "configs/{pair_id}_nisar_cuda.yaml"
  run:
    import yaml

    pair = pairs[wildcards.pair_id]

    with open(input.template) as f:
      cfg =  yaml.safe_load(f)

    cfg["runconfig"]["groups"]["logging"]["path"] = f"/opt/logs/isce3_nisar_log_{wildcards.pair_id}.txt"
    cfg["runconfig"]["groups"]["input_file_group"]["reference_rslc_file"] = f"/opt/inputs/rslc/{pair['ref_rslc']}.h5"
    cfg["runconfig"]["groups"]["input_file_group"]["secondary_rslc_file"] = f"/opt/inputs/rslc/{pair['sec_rslc']}.h5"
    cfg["runconfig"]["groups"]["product_path_group"]["sas_output_file"] = f"/opt/products/{wildcards.pair_id}_product.h5"

    with open(output.cfg, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False)

# download 
rule download:
  output:
    rslc_h5 = str(rslc_output_dir / "{rslc_scene}.h5")
  params:
    url = lambda wildcards: rslc_url_set[wildcards.rslc_scene]
  resources:
    runtime=120,
    mem_mb=20000,
    cpus_per_task=16,
  shell:
    """
    echo "Downloading {wildcards.rslc_scene}"

    wget --no-verbose --netrc -O {output.rslc_h5} "{params.url}"
    """

rule delete_rslc:
  input:
    done = lambda wildcards: [done_path(pair_id) for pair_id in ref_rslc2pairs.get(wildcards.rslc_scene, [])]
  output:
    deleted = str(state_output_dir / "{rslc_scene}.deleted")
  shell:
    """
    echo "Checking if {wildcards.rslc_scene} can be deleted"

    if [ -f {rslc_output_dir}/{wildcards.rslc_scene}.h5 ]; then
    echo "Deleting {wildcards.rslc_scene}.h5"
    rm {rslc_output_dir}/{wildcards.rslc_scene}.h5
    fi

    touch {output.deleted}
    """













# # ── config ────────────────────────────────────────────────────────────────────

# TRIPLETS_JSON  = "state_files/nisar_triplets_track94_frame160.json"
# INPUTS_DIR     = Path("inputs")
# OUTPUTS_DIR    = Path("outputs")
# APPTAINER_IMG  = "isce3.sif"
# RUNCONFIG_DIR  = Path("configs")

# # ── load state ────────────────────────────────────────────────────────────────

# with open(TRIPLETS_JSON) as f:
#     TRIPLETS = json.load(f)

# PAIRS = TRIPLETS["pairs"]

# # Ordered unique scenes derived directly from pairs JSON
# _scene_map = {}
# for pair in PAIRS.values():
#     _scene_map[pair["ref_scene"]] = pair["ref_url"]
#     _scene_map[pair["sec_scene"]] = pair["sec_url"]

# # Sort by the date token at index 11 in the scene name
# SCENES = sorted(
#     [{"name": name, "url": url} for name, url in _scene_map.items()],
#     key=lambda s: s["name"].split("_")[11]  # e.g. 20251104T182146
# )

# # Group pairs by ref_scene (anchor) — preserves sorted order
# PAIR_GROUPS = []
# for anchor, group in groupby(PAIRS.values(), key=lambda p: p["ref_scene"]):
#     PAIR_GROUPS.append(list(group))

# # Flat ordered pair IDs for Snakemake targets
# ALL_PAIR_IDS = [p["pair_id"] for group in PAIR_GROUPS for p in group]

# # ── helpers ───────────────────────────────────────────────────────────────────

# def h5_path(scene_name):
#     return str(INPUTS_DIR / f"{scene_name}.h5")

# def done_path(pair_id):
#     return str(OUTPUTS_DIR / pair_id / "insar.done")

# def runconfig_path(pair_id):
#     return str(RUNCONFIG_DIR / f"{pair_id}.yaml")


# # ── rules ─────────────────────────────────────────────────────────────────────

# rule all:
#     input:
#         expand(done_path("{pair_id}"), pair_id=ALL_PAIR_IDS)


# rule download:
#     """Download a single NISAR RSLC .h5 from ASF."""
#     output:
#         h5 = INPUTS_DIR / "{scene_name}.h5"
#     params:
#         url = lambda wildcards: next(
#             s["url"] for s in SCENES if s["name"] == wildcards.scene_name
#         )
#     shell:
#         """
#         mkdir -p {INPUTS_DIR}
#         echo "Downloading {wildcards.scene_name}"
#         wget --no-verbose --show-progress \
#              --user $ASF_USER --password $ASF_PASSWORD \
#              -O {output.h5} "{params.url}"
#         """


# rule make_runconfig:
#     """Write the isce3 YAML runconfig for a pair."""
#     output:
#         cfg = RUNCONFIG_DIR / "{pair_id}.yaml"
#     run:
#         pair = PAIRS[wildcards.pair_id]
#         cfg  = {
#             "runconfig": {
#                 "name": wildcards.pair_id,
#                 "groups": {
#                     "input_file_group": {
#                         "reference_rslc_file": h5_path(pair["ref_scene"]),
#                         "secondary_rslc_file": h5_path(pair["sec_scene"]),
#                     },
#                     "product_path_group": {
#                         "product_path": str(OUTPUTS_DIR / wildcards.pair_id),
#                         "scratch_path":  str(OUTPUTS_DIR / wildcards.pair_id / "scratch"),
#                     },
#                     "primary_executable": {
#                         "product_type": "GUNW"
#                     }
#                 }
#             }
#         }
#         import yaml
#         Path(output.cfg).parent.mkdir(parents=True, exist_ok=True)
#         Path(output.cfg).write_text(yaml.dump(cfg, default_flow_style=False))


# rule process_pair:
#     """Run isce3 InSAR processing for one pair inside Apptainer."""
#     input:
#         ref_h5 = lambda wildcards: h5_path(PAIRS[wildcards.pair_id]["ref_scene"]),
#         sec_h5 = lambda wildcards: h5_path(PAIRS[wildcards.pair_id]["sec_scene"]),
#         cfg    = runconfig_path("{pair_id}"),
#     output:
#         done   = OUTPUTS_DIR / "{pair_id}" / "insar.done"
#     params:
#         out_dir = lambda wildcards: str(OUTPUTS_DIR / wildcards.pair_id),
#         pair    = lambda wildcards: PAIRS[wildcards.pair_id],
#     shell:
#         """
#         mkdir -p {params.out_dir}

#         echo "Processing pair {wildcards.pair_id}"
#         echo "  ref: {params.pair[ref_scene]}"
#         echo "  sec: {params.pair[sec_scene]}"
#         echo "  interval: {params.pair[interval_days]} days"

#         apptainer exec \
#             --bind {INPUTS_DIR}:/inputs \
#             --bind {params.out_dir}:/output \
#             --bind {RUNCONFIG_DIR}:/runconfig \
#             {APPTAINER_IMG} \
#             python3 /opt/isce3/bin/insar.py \
#                 /runconfig/{wildcards.pair_id}.yaml

#         # Update pair status in state JSON
#         python3 - << 'PYEOF'
# import json
# from pathlib import Path
# state_path = Path("{TRIPLETS_JSON}")
# state = json.loads(state_path.read_text())
# state["pairs"]["{wildcards.pair_id}"]["status"] = "done"
# state_path.write_text(json.dumps(state, indent=2))
# PYEOF

#         touch {output.done}
#         echo "Pair {wildcards.pair_id} complete"
#         """


# rule delete_scene:
#     """Delete a downloaded .h5 once all pairs that use it are done."""
#     input:
#         done = lambda wildcards: [
#             done_path(p["pair_id"])
#             for p in PAIRS.values()
#             if wildcards.scene_name in (p["ref_scene"], p["sec_scene"])
#         ]
#     output:
#         sentinel = INPUTS_DIR / "{scene_name}.deleted"
#     shell:
#         """
#         if [ -f {INPUTS_DIR}/{wildcards.scene_name}.h5 ]; then
#             echo "Deleting {wildcards.scene_name}.h5"
#             rm {INPUTS_DIR}/{wildcards.scene_name}.h5

#             python3 - << 'PYEOF'
# import json
# from pathlib import Path
# state_path = Path("{TRIPLETS_JSON}")
# state = json.loads(state_path.read_text())
# if "{wildcards.scene_name}" in state.get("scenes", {}):
#     state["scenes"]["{wildcards.scene_name}"]["status"] = "deleted"
#     state_path.write_text(json.dumps(state, indent=2))
# PYEOF
#         fi
#         touch {output.sentinel}
#         """












"""
For Claude

Here's a concise summary you can paste in as context:

NISAR InSAR Processing Pipeline — Context Summary
Project location
/hpc22/isce3-builder/ with existing: isce3.sif, utils/, state_files/, inputs/, outputs/, configs/
What we've built so far
utils/discovery.py — queries ASF for NISAR RSLC scenes by track/frame using asf_search. Writes:
state_files/nisar_rslc_state_track{TRACK}_frame{FRAME}.json
Schema:
json{
  "scenes": {
    "<scene_name>": {
      "startTime": "2025-11-04T18:21:46Z",
      "stopTime":  "...",
      "pathNumber": 94,
      "frameNumber": 160,
      "url": "https://nisar.asf.earthdatacloud.nasa.gov/.../.h5",
      "s3Urls": [...]
    }
  }
}
utils/build_triplets.py — reads discovery JSON, builds interferogram pairs at exact 12, 24, and 36 day temporal baselines using date.fromisoformat(props["startTime"][:10]). If a scene is missing at a given interval, that pair is silently skipped. Writes:
state_files/nisar_triplets_track{TRACK}_frame{FRAME}.json
Schema:
json{
  "metadata": { "intervals_days": [12, 24, 36], "total_pairs": 10 },
  "pairs": {
    "2025-11-04_2025-11-16": {
      "pair_id": "2025-11-04_2025-11-16",
      "ref_scene": "NISAR_L1_PR_RSLC_004_...",
      "sec_scene": "NISAR_L1_PR_RSLC_005_...",
      "ref_date": "2025-11-04",
      "sec_date": "2025-11-16",
      "interval_days": 12,
      "ref_url": "https://...",
      "sec_url": "https://...",
      "status": "pending"
    }
  }
}
Snakefile — orchestrates the full sliding window pipeline. At startup it derives an ordered unique scene list from the pairs JSON (sorted by date token at index 11 of the scene name, e.g. 20251104T182146), groups pairs by anchor/ref scene, then executes serially:
download scenes[0:4]
process pair 1  →  process pair 2  →  process pair 3
save state (mark pair status = "done" in JSON)
delete scenes[0]  (once all pairs referencing it are done)
download scenes[4]
process next 3 pairs
...
Four rules: download (wget with $ASF_USER/$ASF_PASSWORD), make_runconfig (writes isce3 YAML), process_pair (runs apptainer exec isce3.sif), delete_scene (rm .h5, touch .deleted sentinel).
Processing is serial not parallel — one Apptainer job at a time, no Slurm yet.
Key design decisions

No SQLite — all state lives in JSON files
build_triplets.py is intentionally simple — no CLI args, paths hardcoded at top
Gap handling is implicit — if a scene doesn't exist at a given interval, no pair is created
Snakemake deletion is dependency-driven — a scene is only deleted once all pairs that reference it (as either ref or sec) have a .done file
The delete_scene rule won't run automatically unless .deleted sentinels are added to rule all's inputs

Still to do

Weekly cron job to re-run discovery + build_triplets + snakemake
Slurm integration for process_pair (currently runs on login node)
ASF credentials handling
Confirm correct isce3 entry point (/opt/isce3/bin/insar.py is a placeholder)
Test with real downloads
"""