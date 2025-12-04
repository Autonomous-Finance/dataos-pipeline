DROP TABLE IF EXISTS default.blocks;
SET allow_experimental_object_type=1;

CREATE TABLE default.blocks
(
    height            Integer,
    block_timestamp   DateTime,
    inserted_at       DateTime,
    data_raw          JSON,
    data_raw_str      String
)
ENGINE ReplacingMergeTree
ORDER BY (height)
;


SET output_format_json_named_tuples_as_objects = 1;

SELECT data_raw FROM default.blocks FORMAT JSONEachRow;