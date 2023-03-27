# SLIMS module for Cellophane

Fetch sample information from a SLIMS database. If samples are present (from a previous module or a samples file) these samples will be augmented with information from slims. If one or more sample IDs are specified they will be searched for. If no samples are present and no sample IDs are specified all samples matching the specified criteria will be fetched.

Sometimes the records holding sample information are derived from a parent record holding information linking the sample to a specific analysis. In this case the `derived_from.*` parameters can be used to initially search for parent objects that sample objects need to be derived from.

Optionally, the `bioinfo.*` parameters can be used to add/update/check "biofinformatics" objects to track the state of a secondary analysis. On pipeline completion/failure the specified field on the bioinformatics objects will be updated. to reflect this state. The state of these objects can then be checked by the wrapper to determine what samples should be excluded from analysis.

## Configuration

Option                      | Type      | Required | Default | Description
----------------------------|-----------|----------|---------|-------------
`url`                       | str       | x        |         | SLIMS Server URL
`username`                  | str       | x        |         | SLIMS username
`password`                  | str       | x        |         | SLIMS password
`content_type`              | int       | x        |         | Content type PK for sample records
`criteria`                  | str       | x        |         | SLIMS criteria for finding records (see [Criteria](#Criteria))
`map_field`                 | list[str] |          |         | Mapping of keys to SLIMS field(s) (see [Fields](#Fields)/[Mappings](#Mappings))
`derived_from.criteria`     | str       |          |         | SLIMS criteria for finding parent samples (see [Criteria](#Criteria))
`derived_from.content_type` | int       |          |         | Content type PK for parent records
`bioinfo.content_type`      | int       |          |         | Content type PK for bioinformatics records
`bioinfo.state_field`       | str       |          |         | Field with state of bioinformatics objects (see [Fields](#Fields))
`bioinfo.create`            | bool      |          | false   | Create bioinformatics objects
`bioinfo.check`             | bool      |          | false   | Check state of existing bioinformatics records
`bioinfo.check_criteria`    | str       |          |         | Criteria for checking completed bioinformatics (see [Criteria](#Criteria))
`id`                        | list[str] |          |         | Manually select SLIMS Sample ID(s)
`allow_duplicates`          | bool      |          | false   | Allow duplicate samples (eg. if a pre-hook can handle this)
`dry_run`                   | bool      |          | false   | Do not create/update SLIMS bioinformatics objects
`novel_max_age`             | str       |          | 1 year  | Maximum age of novel samples (eg. "4 days", "2 months", "1 year")

## Criteria

Criteria are specified using `<FIELD> <OPERATOR> <VALUE>` syntax where operators take 1, 2, or more values.

> **WARNING** Invalid field names are silently ignored. This makes it very easy to accidentally fetch more samples than expected. This behavior is inherent to the official SLIMS API python bindings.

The following operators are supported:

- `equals` / `equals_ignore_case` / `not_equals` / `not_equals_ignore_case` (1 value)
- `one_of` / `not_one_of` (n values)
- `contains` / `not_contains` (n values)
- `starts_with` / `ends_with` / `not_starts_with` / `not_ends_with` (1 value)
- `between_inclusive` / `not_between_inclusive` (2 values)
- `greater_than` / `less_than` (1 value)

Complex boolean criteria can be constructed using `and`/`or` and parentheses.

eg. `cntn_cstm_foo equals a or (cntn_cstm_foo equals b and cntn_cstm_bar not_between_inclusive c d)`

## Fields

Fields are specified using the full SLIMS field name (eg. `cntn_fk_contentType`). Sub-fields inside JSON fields can be accessed by using the `json:` prefix and dot-notation (eg. json:cntn_cstm_foo.bar.baz) 

## Mappings

SLIMS fields can be mapped to keys on the `data.Samples` object using `<KEY>=<FIELD>` with the same field syntax as above. This can be used to override any field on the `data.Samples` object (eg. `files=json:cntn_cstm_myJSONField.fastq_paths`).
