#!/bin/bash

set -e -o pipefail

eval ${UNPACK_INIT}

echo "CMD: petasuite \
--decompress \
--numthreads $THREADS \
$COMPRESSED_PATH"

petasuite \
    --decompress \
    --numthreads $THREADS \
    $COMPRESSED_PATH

eval ${UNPACK_EXIT}