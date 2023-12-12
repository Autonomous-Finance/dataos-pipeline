
CREATE TABLE dataos_explore.meta_keybert_keywords
(
    id            String,
    created_at    DateTime,
    keywords      String
)
ENGINE ReplacingMergeTree
ORDER BY (created_at, id)
;

ALTER TABLE dataos_explore.meta_keybert_keywords ADD INDEX id_idx id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE dataos_explore.meta_keybert_keywords MATERIALIZE INDEX id_idx;
