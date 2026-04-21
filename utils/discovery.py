import json
import argparse
from pathlib import Path
import asf_search as asf

def build_nisar_discovery(track=94, frame=160, direction='ASCENDING'):

    """
    Take a NISAR track and frame and build a json state showing all available RSLCs + download links (ASF/AWS S3)
    """

    root = Path(__file__).resolve().parent.parent
    output_dir = root / "state_files" / direction / str(track) / str(frame)
    output_path = output_dir / f"nisar_rslc_state_track{track}_frame{frame}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    opts = asf.ASFSearchOptions(**{
        "processingLevel": [
            "RSLC"
        ],
        "dataset": [
            "NISAR"
        ],
        "relativeOrbit": [
            track
        ],
        "frame": [
            frame
        ],
        "mainBandPolarization": [
            'HH+HV'
        ]
    })

    results = asf.search(flightDirection=direction, opts=opts)

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

def main():
    parser = argparse.ArgumentParser(description="NISAR data discovery via ASF")

    parser.add_argument(
        "--direction", type=str, required=True,
        help="NISAR flight direction [ASCENDING|DESCENDING]"
    )
    parser.add_argument(
        "--track", type=int, required=True,
        help="NISAR track number"
        )
    parser.add_argument(
        "--frame", type=int, required=True,
        help="NISAR frame number"
        )

    args = parser.parse_args()

    build_nisar_discovery(track=args.track, frame=args.frame, direction=args.direction)

if __name__ == '__main__':
    main()