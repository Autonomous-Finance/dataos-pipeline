
create table dataos_explore.meta_text_embeddings
(
    id                   String,
    embedding            Array(Float64),
    created_at           DateTime
)
engine = ReplacingMergeTree ORDER BY (created_at, id)
SETTINGS index_granularity = 8192;
