# Unpacking module for Cellophane

Module for unpacking compressed files. Supports SPRING (.spring) and Petagene (.fasterq) compressed FASTQ via SGE.

## Configuration

Option             | Type | Required | Default | Description
-------------------|------|----------|---------|-------------
`unpack.threads`   | int  |          | 40      | SGE slots unpacking 


## Hooks

Name     | When | Condition | Description
---------|------|-----------|-------------
`unpack` | Pre  |           | Extract sample files
