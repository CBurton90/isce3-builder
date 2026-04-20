import argparse
import json
import subprocess
import sys
from pathlib import Path
"""
An orchestrator for sliding window RSLC isce3 processing to reduce data storage requirements

Step 1 - build a list of all reference RSLCs from our state file
Step 2 - for the first reference RSLC in the list find all possible pairs (i.e. a batch) that require processing (for 12/24/36 day IFGs this will be a maximum of 3 pairs in a batch but could be less if data is missing)
Step 3 - download all RSLC scenes (ref/sec) required to process the pairs in a batch (Snakemake)
Step 4 - run processing (Snakemake)
Step 5 - delete the reference RSLC for this batch as it is no longer required (Snakemake)
Step 6 - move to the next reference RSLC in the list and begin the process again from Step 2
"""

root = Path.cwd()
state_output_dir = root / "state_files" # where we store state files

def load_rslc_pairs(path):
    with open(path) as f:
        pairs = json.load(f)
    return pairs["pairs"]

def build_ref_rslc_order(pairs):
    ordered_ref_rslcs = {}
    for pair in pairs.values():
        ref = pair["ref_rslc"]
        if ref not in ordered_ref_rslcs:
            ordered_ref_rslcs[ref] = ref.split("_")[11]
    return sorted(ordered_ref_rslcs.keys(), key=lambda s: ordered_ref_rslcs[s])

def build_ref_rslc2pairs(pairs):
    ref_rslc2pairs = {}
    for pair_id, pair in pairs.items():
        rslc_scene_id = pair["ref_rslc"]
        ref_rslc2pairs.setdefault(rslc_scene_id, []).append(pair_id)
    return ref_rslc2pairs

def done_path(pair):
    return str(state_output_dir / f"{pair}_nisar.done")

def run_snakemake(targets, jobs, local_cores, dry_run):

    cmd = [
        "snakemake",
        "--executor", "slurm",
        "--jobs", str(jobs),
        "--local-cores", str(local_cores),
        ]
    
    if dry_run:
        cmd.append("-n")
        cmd.append("--printshellcmds")
    
    cmd += targets

    result = subprocess.run(cmd)

    return result.returncode == 0

def main():
    parser = argparse.ArgumentParser(description="NISAR RSLC batch orchestration for Snakemake")

    parser.add_argument(
        "--dry-run", action="store_true",
        help="Snakemake dry-run, useful to test if the workflow is defined properly and to estimate the amount of needed computation."
        )
    parser.add_argument(
    "--pairs-state", type=Path,
    help="Path to json file containing NISAR RSLC pairs to be processed"
    )
    parser.add_argument(
        "--jobs", type=int, default=1,
        help="Number of jobs to pass to Snakemake, default is 1 for single GPU"
    )
    parser.add_argument(
        "--local-cores", type=int, default=4,
        help="Number of CPU cores to use on the login node, default is 4"
    )

    args = parser.parse_args()

    if not args.pairs_state.exists():
        print("Json pair state not found, exiting")
        sys.exit(1)

    pairs = load_rslc_pairs(args.pairs_state)
    ref_rslc_order = build_ref_rslc_order(pairs)
    ref_rslc2pairs = build_ref_rslc2pairs(pairs)

    # print(ref_rslc_order)
    # print(ref_rslc2pairs)

    for batch_idx, ref_rslc_id in enumerate(ref_rslc_order, start=1):

        # print(batch_idx)
        # print(ref_rslc_id)

        pair_ids = ref_rslc2pairs[ref_rslc_id]
        targets = [str(done_path(p)) for p in pair_ids]
        targets.append(str(state_output_dir / f"{ref_rslc_id}.deleted"))
        
        success =  run_snakemake(targets, args.jobs, args.local_cores, args.dry_run)

        if not success:
            print("Snakemake failed on batch {batch_idx}, exiting")
            sys.exit(1)

if __name__ == '__main__':
    main()

    