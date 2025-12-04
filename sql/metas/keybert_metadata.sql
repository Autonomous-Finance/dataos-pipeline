CREATE TABLE dataos_explore.meta_keybert_keywords_new
(
    id            String,
    created_at    DateTime,
    keywords      String
)
ENGINE ReplacingMergeTree
ORDER BY (id, created_at)
;

-- Copy data from the old table to the new one
INSERT INTO dataos_explore.meta_keybert_keywords_new SELECT * FROM dataos_explore.meta_keybert_keywords;

-- Drop the old table (make sure you have a backup before doing this)
DROP TABLE dataos_explore.meta_keybert_keywords;

-- Rename the new table to the original name
RENAME TABLE dataos_explore.meta_keybert_keywords_new TO dataos_explore.meta_keybert_keywords;

-- Re-create the index on the modified table
ALTER TABLE dataos_explore.meta_keybert_keywords ADD INDEX id_idx id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE dataos_explore.meta_keybert_keywords MATERIALIZE INDEX id_idx;
