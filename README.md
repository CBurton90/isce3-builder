# isce3-builder
NISAR IFG processing with isce3 and Docker.

## Docker build

### testing

`sudo docker build --target tester -t isce3:testing . --progress=plain`

DEM stage will fail -> bbox_epsg parameter out of sync between prod code and tests.

### final

`sudo docker build --target final -t isce3:0.25.7 . --progress=plain`

## Docker compose (recommended)
