
create table dataos_explore.meta_text_summary
(
    id                   String,
    summary              String,
    created_at           DateTime
)
engine = ReplacingMergeTree ORDER BY (created_at, id)
SETTINGS index_granularity = 8192;
