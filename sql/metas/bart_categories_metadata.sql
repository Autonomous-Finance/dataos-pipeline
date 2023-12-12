
CREATE TABLE dataos_explore.meta_bart_categories
(
    id                String,
    created_at        DateTime,
    categories        Map(String, Float64),
    top_categories    String
)
ENGINE ReplacingMergeTree
ORDER BY (created_at, id)
;

ALTER TABLE dataos_explore.meta_bart_categories ADD INDEX id_idx id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE dataos_explore.meta_bart_categories MATERIALIZE INDEX id_idx;
