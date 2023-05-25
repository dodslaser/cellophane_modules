#!/bin/bash

set -e -o pipefail

module load spring

extract_path="$(dirname ${COMPRESSED_PATH})/$(basename -s 'spring' ${COMPRESSED_PATH}).fastq.gz"

echo "spring \
--decompress \
--gzipped-fastq \
--working-dir $TMPDIR \
--output-file $extract_path \
--input-file $COMPRESSED_PATH \
--num-threads $THREADS"

spring \
    --decompress \
    --gzipped-fastq \
    --working-dir $TMPDIR \
    --output-file $extract_path \
    --input-file $COMPRESSED_PATH \
    --num-threads $THREADS
