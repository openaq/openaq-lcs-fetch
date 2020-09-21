#!/usr/bin/env bash
# based on github.com/mapbox/lambda-cfn

# ./util/upload-lambda lambda-directory bucket/prefix

GITSHA=$(git rev-parse HEAD)
REPO=$(basename $(git rev-parse --show-toplevel))
echo "ok - ${GITSHA}"

yarn install
zip -qr /tmp/${GITSHA}.zip *

aws s3 cp /tmp/${GITSHA}.zip s3://devseed-artifacts/${REPO}/lambda-${GITSHA}.zip

rm /tmp/${GITSHA}.zip
