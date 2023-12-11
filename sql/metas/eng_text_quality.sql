
CREATE TABLE dataos_explore.meta_eng_data_quality
(
    id String,
    retrieved_at DateTime,
    created_at DateTime,
    processed_at DateTime,
    eng_quality Float32,
    content_short String
)
ENGINE ReplacingMergeTree
ORDER BY (retrieved_at, created_at, id)
;

ALTER TABLE dataos_explore.meta_eng_data_quality ADD INDEX id_idx id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE dataos_explore.meta_eng_data_quality MATERIALIZE INDEX id_idx;