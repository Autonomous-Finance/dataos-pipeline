drop table if exists dataos_explore.video_transcriptions;
create table dataos_explore.video_transcriptions
(
    source_url String,
    download_url String,
    created_at Datetime,
    transcription_json String,
    fulltext String,
    length_in_s Int
)
engine = ReplacingMergeTree ORDER BY (source_url)
SETTINGS index_granularity = 4096;
