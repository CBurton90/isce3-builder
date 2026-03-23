#!/bin/bash
# Download Copernicus GLO-30 DEM tiles for NZ North Island

OUTPUT_DIR="/opt/dem"
BUCKET="s3://copernicus-dem-30m"

mkdir -p "$OUTPUT_DIR"

for lat in S35 S36 S37 S38 S39; do
    for lon in E172 E173 E174 E175 E176 E177 E178; do
        tile="Copernicus_DSM_COG_10_${lat}_00_${lon}_00_DEM"
        tif="${tile}.tif"
        if aws s3 cp "${BUCKET}/${tile}/${tif}" "${OUTPUT_DIR}/${tif}" --no-sign-request 2>/dev/null; then
            echo "OK: $tif"
        else
            echo "SKIP: $tif"
        fi
    done
done

echo "Building VRT mosaic..."
gdalbuildvrt "${OUTPUT_DIR}/dem_egm2008.vrt" "${OUTPUT_DIR}"/*.tif

echo "Converting EGM2008 -> WGS84 ellipsoid..."
gdalwarp -s_srs EPSG:4326+3855 -t_srs EPSG:4326 \
    "${OUTPUT_DIR}/dem_egm2008.vrt" \
    "${OUTPUT_DIR}/dem_wgs84.vrt" \
    -of VRT

echo "Done. DEM ready at ${OUTPUT_DIR}/dem_wgs84.vrt"
