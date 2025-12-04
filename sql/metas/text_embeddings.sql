CREATE TABLE dataos_explore.meta_text_embeddings
(
    id                   String,
    embedding            Array(Float64),
    created_at           DateTime
)
ENGINE = ReplacingMergeTree ORDER BY (id, created_at)
SETTINGS index_granularity = 8192;