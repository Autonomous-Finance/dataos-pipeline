CREATE TABLE dataos_explore.meta_bart_categories
(
    id                String,
    created_at        DateTime,
    categories        Map(String, Float64),
    top_categories    String
)
ENGINE ReplacingMergeTree
ORDER BY (id, created_at)
;