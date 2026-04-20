import json
from pathlib import Path
import asf_search as asf

def build_nisar_discovery(track=94, frame=160):

    """
    Take a NISAR track and frame and build a json state showing all available RSLCs + download links (ASF/AWS S3)
    """

    root = Path(__file__).resolve().parent.parent
    output_dir = root / "state_files"
    output_path = output_dir / f"nisar_rslc_state_track{TRACK}_frame{FRAME}.json"

    opts = asf.ASFSearchOptions(**{
        "processingLevel": [
            "RSLC"
        ],
        "dataset": [
            "NISAR"
        ],
        "relativeOrbit": [
            TRACK
        ],
        "frame": [
            FRAME
        ],
        "mainBandPolarization": [
            'HH+HV'
        ]
    })

    results = asf.search(opts=opts)

    discovered_frames = {"scenes": {}}

    for feature in results:
        prop = feature.properties
        scene = prop["sceneName"]

        discovered_frames["scenes"][scene] = {
            "startTime": prop["startTime"],
            "stopTime": prop["stopTime"],
            "pathNumber": prop["pathNumber"],
            "frameNumber": prop["frameNumber"],
            "url": prop["url"],
            "s3Urls": prop["s3Urls"],
            }

    with open(output_path, "w") as f:
        json.dump(discovered_frames, f, indent=2)

if __name__ == '__main__':
    TRACK = 94 # hardcoded global vars, make dynamic at some point
    FRAME = 160
    build_nisar_discovery(track=TRACK, frame=FRAME)