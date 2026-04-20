import json
from datetime import date, timedelta
from pathlib import Path

def pair_gen(discovery_state_dict, tb_intervals):

    rslc_temporal_dict = {}

    for scene, scene_props in discovery_state_dict.items():
        rslc_temporal_dict[date.fromisoformat(scene_props["startTime"][:10])] = {"scene": scene, "url": scene_props["url"]}

    pairs_dict = {}

    # using 12 day intervals at 12, 24, and 36 day baselines, could be an issue if NISAR acquistion(s) ever occur at slightly less/longer temporal baselines?
    scenes_ordered = sorted(rslc_temporal_dict)
    col = {d: i * 22 for i, d in enumerate(scenes_ordered)} # just for pretty printing

    for ref_date in scenes_ordered:
        ref_rslc = rslc_temporal_dict[ref_date]
        for tb in tb_intervals:
            sec_date = ref_date + timedelta(days=tb)
            if sec_date not in rslc_temporal_dict:
                continue
            sec_rslc = rslc_temporal_dict[sec_date]
            
            # print IFG temporal baselines
            start = col[ref_date]
            end = col[sec_date]
            dashes = '-' * (end - start - 10)
            print(f"{' ' * start}{ref_date}{dashes}{sec_date} ({tb}-day IFG)")

            pair_id = f"{ref_date}_{sec_date}"

            pairs_dict[pair_id] = {
                "ref_rslc": ref_rslc["scene"],
                "sec_rslc": sec_rslc["scene"],
                "ref_url": ref_rslc["url"],
                "sec_url": sec_rslc["url"],
            }

    return pairs_dict

def build_nisar_rslc_pairs(track=94, frame=160, tb_intervals=[12, 24, 36]):

    """
    Take a NISAR RSLC discovery state Json and build all possible interferometric pairs.
    The maximum temporal baseline we desire is 36 days therefore for each reference RSLC
    in the discovery state we then choose a secondary RSLC at 12, 24, and 36 days (3
    interferometric pairs per reference).
    """
    
    root = Path(__file__).resolve().parent.parent
    output_dir = root / "state_files"
    input_path = output_dir / f"nisar_rslc_state_track{TRACK}_frame{FRAME}.json"
    output_path = output_dir / f"nisar_rslc_ifg_pairs_track{TRACK}_frame{FRAME}.json"

    discovery_json = json.loads(input_path.read_text())

    pair_state = pair_gen(discovery_json["scenes"], tb_intervals)

    output = {
        "rslc_pair_metadata": {
            "temporal_baselines": tb_intervals,
            "total_possible_IFGs": len(pair_state),
        },
        "pairs": pair_state,
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

if __name__ == '__main__':
    TRACK = 94 # hardcoded global vars, make dynamoic at some point
    FRAME = 160
    build_nisar_rslc_pairs(track=TRACK, frame=FRAME, tb_intervals=[12, 24, 36])