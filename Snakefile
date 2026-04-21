"""
NISAR InSAR Snakemake Pipeline
"""

import json
from pathlib import Path

TRACK = config["track"]
FRAME = config["frame"]
DIRECTION = config["direction"]
OUTPUT_PROD_PATH = config["product_path"]
IMAGE = "isce3cuda.sif"

root = Path.cwd()
state_output_dir = root / "state_files" / DIRECTION / str(TRACK) / str(FRAME) # where we store json state files
rslc_output_dir = root / "inputs" / DIRECTION / str(TRACK) / str(FRAME) / "rslc" # where we store NISAR RSLC HDF5 scenes
rslc_output_dir.mkdir(parents=True, exist_ok=True)
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
    cfg = f"configs/{DIRECTION}/{TRACK}/{FRAME}/{{pair_id}}_nisar_cuda.yaml"
  output:
    done = done_path("{pair_id}")
  resources:
    mem_mb=100000,
    cpus_per_task=32,
    gres="gpu:3g.40gb:1",
    runtime=120,
  shell:
    """
    mkdir -p {OUTPUT_PROD_PATH}/products/{DIRECTION}/{TRACK}/{FRAME}
    mkdir -p ./configs/{DIRECTION}/{TRACK}/{FRAME}
    mkdir -p ./logs/{DIRECTION}/{TRACK}/{FRAME}

    ml apptainer

    apptainer exec --nv \
      --pwd /opt \
      --writable-tmpfs \
      --bind ./inputs/{DIRECTION}/{TRACK}/{FRAME}:/opt/inputs \
      --bind ./outputs:/opt/outputs \
      --bind {OUTPUT_PROD_PATH}/products/{DIRECTION}/{TRACK}/{FRAME}:/opt/products \
      --bind ./dem:/opt/dem \
      --bind ./configs/{DIRECTION}/{TRACK}/{FRAME}:/opt/configs \
      --bind ./qa:/opt/qa \
      --bind ./logs/{DIRECTION}/{TRACK}/{FRAME}:/opt/logs \
      --bind ./patches/geocode_insar.py:/opt/isce3/packages/nisar/workflows/geocode_insar.py \
      {IMAGE} \
      python3 -u /opt/isce3/packages/nisar/workflows/insar.py \
      /opt/configs/{wildcards.pair_id}_nisar_cuda.yaml \
      > logs/{DIRECTION}/{TRACK}/{FRAME}/isce3_nisar_bash_log_{wildcards.pair_id}.txt 2>&1

    touch {output.done}
    """

# build specific YAML config to process an RSLC pair
rule make_runconfig:
  input:
    template = "configs/insar-cuda-template.yaml"
  output:
    cfg = f"configs/{DIRECTION}/{TRACK}/{FRAME}/{{pair_id}}_nisar_cuda.yaml"
  run:
    import yaml

    pair = pairs[wildcards.pair_id]

    with open(input.template) as f:
      cfg =  yaml.safe_load(f)

    cfg["runconfig"]["groups"]["logging"]["path"] = f"/opt/logs/isce3_nisar_journal_log_{wildcards.pair_id}.txt"
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