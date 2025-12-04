DROP TABLE IF EXISTS dataos_explore.indexable;
CREATE TABLE IF NOT EXISTS dataos_explore.indexable
(
        id String,
        created_at DateTime,
        created_at_dt DateTime,
        data_size Int64,
        app String,
        INDEX id_idx id TYPE bloom_filter GRANULARITY 1,
        INDEX app_idx app TYPE set(100) GRANULARITY 1
)
ENGINE ReplacingMergeTree
ORDER BY (created_at_dt, id)
;


DROP TABLE IF EXISTS dataos_explore.file_cache_2;
CREATE TABLE IF NOT EXISTS dataos_explore.file_cache_2
(
        id String,
        created_at DateTime,
        created_at_dt DateTime,
        retrieved_at DateTime,
        content String,
        INDEX id_idx id TYPE bloom_filter GRANULARITY 1
)
ENGINE ReplacingMergeTree
ORDER BY (created_at_dt, id)
;

DROP TABLE IF EXISTS dataos_explore.file_cache_tracker;
CREATE MATERIALIZED VIEW dataos_explore.file_cache_tracker
ENGINE MergeTree
ORDER BY retrieved_at
AS
(
    SELECT id, created_at_dt, created_at, app, retrieved_at, data_size
    FROM dataos_explore.file_cache
    JOIN dataos_explore.indexable USING (id)
);


SELECT COUNT(1) FROM dataos_explore.file_cache2;

INSERT INTO dataos_explore.file_cache_2(id, created_at, created_at_dt, content, retrieved_at) SELECT id, created_at, toStartOfDay(created_at), content, now() FROM dataos_explore.file_cache;
DROP TABLE dataos_explore.file_cache;
RENAME TABLE dataos_explore.file_cache_2 TO dataos_explore.file_cache;


INSERT INTO dataos_explore.indexable(id, created_at, created_at_dt, data_size, app)
SELECT id, created_at, toStartOfDay(created_at), data_size, tag_app_agg
FROM prod.transactions_silver_tbl
WHERE created_at > (SELECT MAX(created_at) FROM (SELECT created_at FROM dataos_explore.indexable ORDER BY created_at_dt DESC LIMIT 1))
AND multiSearchAnyCaseInsensitive(tag_app_agg, ['permapage-post']) OR tag_app_agg IN ('paragraph', 'mirrorxyz', 'hey.xyz', 'cyberconnect', 'lenster');

OPTIMIZE TABLE dataos_explore.indexable FINAL;
OPTIMIZE TABLE dataos_explore.file_cache FINAL;


WITH q AS (
    SELECT id
    FROM dataos_explore.indexable
    ANTI JOIN dataos_explore.file_cache f USING (created_at_dt, id)
    LIMIT 1000
)
SELECT DISTINCT id
FROM q
ORDER BY randCanonical()
LIMIT 100;

SELECT count(1) FROM dataos_explore.content_mirrorxyz_v2;


-- TODO:
-- check origina_content_digest and digest for possible deduplication!
SET allow_experimental_nlp_functions=1;
drop table dataos_explore.content_mirrorxyz_v2;
-- todo parameterized view, subset by retreived_at
CREATE TABLE dataos_explore.content_mirrorxyz_v2
ENGINE ReplacingMergeTree
ORDER BY (retrieved_at, id)
AS
(
SELECT
    id,
    created_at,
    JSON_VALUE(f.content, '$.content.body') AS body,
    JSON_VALUE(f.content, '$.content.timestamp') AS content_timestamp,
    JSON_VALUE(f.content, '$.content.title') AS title,
    JSON_VALUE(f.content, '$.authorship.contributor') AS contributor,
    ---
    ngramMinHashCaseInsensitive(body) AS ngram_min_hash,
    ngramSimHashCaseInsensitive(body) AS ngram_hash,
    detectLanguageMixed(body) AS languages,
    length(alphaTokens(body)) AS alpha_token_count,
    length(arrayStringConcat(alphaTokens(body), '')) AS alpha_char_count,
    length(body) AS total_char_count,
    total_char_count - alpha_char_count AS non_alpha_char_count,
    non_alpha_char_count / total_char_count AS non_alpha_ratio,
    alpha_char_count / total_char_count AS alpha_ratio,
    retrieved_at
FROM dataos_explore.file_cache as f
JOIN dataos_explore.indexable USING (id)
WHERE app = 'mirrorxyz'
);


SELECT COUNT(1) FROM dataos_explore.lens_posts;

DROP TABLE IF EXISTS dataos_explore.lens_posts SYNC;
CREATE TABLE dataos_explore.lens_posts
ENGINE ReplacingMergeTree
ORDER BY (retrieved_at, id)
AS
(
SELECT
    id,
    created_at,
    JSON_VALUE(f.content, '$.content') AS body,
    JSON_VALUE(f.content, '$.createdOn') AS content_timestamp,
    JSON_VALUE(f.content, '$.name') AS title,
    JSON_VALUE(f.content, '$.external_url') AS contributor,
    JSONExtractArrayRaw(JSON_VALUE(f.content, '$.tags')) AS tags,
    ---
    ngramMinHashCaseInsensitive(body) AS ngram_min_hash,
    ngramSimHashCaseInsensitive(body) AS ngram_hash,
    detectLanguageMixed(body) AS languages,
    length(alphaTokens(body)) AS alpha_token_count,
    length(arrayStringConcat(alphaTokens(body), '')) AS alpha_char_count,
    length(body) AS total_char_count,
    total_char_count - alpha_char_count AS non_alpha_char_count,
    non_alpha_char_count / total_char_count AS non_alpha_ratio,
    alpha_char_count / total_char_count AS alpha_ratio,
    retrieved_at
FROM dataos_explore.file_cache AS f
JOIN dataos_explore.indexable USING (id)
WHERE app IN ('lenster', 'hey.xyz') AND JSON_VALUE(f.content, '$.mainContentFocus') = 'TEXT_ONLY'
);


-- PERMAPAGES:
-- USE clickhouse: extractTextFromHTML
SELECT extractTextFromHTML(content) FROM dataos_explore.file_cache WHERE id = 'mig6abXkztimz1KWMqOnDkVB6RFlLVJMhwVHclrS37A'

SELECT *
FROM dataos_explore.file_cache AS f
JOIN dataos_explore.indexable USING (created_at_dt, id)
WHERE app LIKE 'permapage-post%'
LIMIT 100;

DROP TABLE IF EXISTS dataos_explore.paragraph SYNC;
CREATE TABLE dataos_explore.paragraph
ENGINE ReplacingMergeTree
ORDER BY (retrieved_at, id)
AS
(
SELECT
    id,
    created_at,
    JSON_VALUE(f.content, '$.markdown') AS body,
    JSON_VALUE(f.content, '$.publishedAt') AS content_timestamp,
    JSON_VALUE(f.content, '$.title') AS title,
    JSON_VALUE(f.content, '$.authors[0]') AS contributor,
    JSONExtractArrayRaw(JSON_VALUE(f.content, '$.categories')) AS tags,
    ---
    ngramMinHashCaseInsensitive(body) AS ngram_min_hash,
    ngramSimHashCaseInsensitive(body) AS ngram_hash,
    detectLanguageMixed(body) AS languages,
    length(alphaTokens(body)) AS alpha_token_count,
    length(arrayStringConcat(alphaTokens(body), '')) AS alpha_char_count,
    length(body) AS total_char_count,
    total_char_count - alpha_char_count AS non_alpha_char_count,
    non_alpha_char_count / total_char_count AS non_alpha_ratio,
    alpha_char_count / total_char_count AS alpha_ratio,
    retrieved_at
FROM dataos_explore.file_cache AS f
JOIN dataos_explore.indexable USING (id)
WHERE app = 'paragraph'
);

SELECT * FROM dataos_explore.cyberconnect WHERE length(tokens(body)) > 100 LIMIT 100;

DROP TABLE IF EXISTS dataos_explore.cyberconnect;
CREATE TABLE dataos_explore.cyberconnect
ENGINE ReplacingMergeTree
ORDER BY (retrieved_at, id)
AS
(
    SELECT
        id,
        created_at,
        JSON_VALUE(content_json, '$.body') AS body,
        JSON_VALUE(content_json, '$.ts') AS content_timestamp,
        JSON_VALUE(content_json, '$.title') AS title,
        JSON_VALUE(content_json, '$.handle') AS contributor,
        JSON_VALUE(content_json, '$.author') AS address,
        ---
        ngramMinHashCaseInsensitive(body) AS ngram_min_hash,
        ngramSimHashCaseInsensitive(body) AS ngram_hash,
        detectLanguageMixed(body) AS languages,
        length(alphaTokens(body)) AS alpha_token_count,
        length(arrayStringConcat(alphaTokens(body), '')) AS alpha_char_count,
        length(body) AS total_char_count,
        total_char_count - alpha_char_count AS non_alpha_char_count,
        non_alpha_char_count / total_char_count AS non_alpha_ratio,
        alpha_char_count / total_char_count AS alpha_ratio,
        retrieved_at
    FROM (
        SELECT *, toString(JSONExtractString(content, 'content')) AS content_json
        FROM dataos_explore.file_cache
        JOIN dataos_explore.indexable USING (id)
        WHERE app = 'cyberconnect'
    )
    WHERE JSON_VALUE(content_json, '$.op') = 'post'
);

DROP TABLE IF EXISTS dataos_explore.odysee;
CREATE MATERIALIZED VIEW dataos_explore.odysee
ENGINE ReplacingMergeTree
ORDER BY (retrieved_at, id)
POPULATE AS
(
    SELECT
        source_url AS id,
        v.created_at AS created_at,
        fulltext AS body,
        '' AS content_timestamp,
        '' AS title,
        '' AS contributor,
        '' AS address,
        ---
        ngramMinHashCaseInsensitive(body) AS ngram_min_hash,
        ngramSimHashCaseInsensitive(body) AS ngram_hash,
        detectLanguageMixed(body) AS languages,
        length(alphaTokens(body)) AS alpha_token_count,
        length(arrayStringConcat(alphaTokens(body), '')) AS alpha_char_count,
        length(body) AS total_char_count,
        total_char_count - alpha_char_count AS non_alpha_char_count,
        non_alpha_char_count / total_char_count AS non_alpha_ratio,
        alpha_char_count / total_char_count AS alpha_ratio,
        v.created_at AS retrieved_at
    FROM dataos_explore.video_transcriptions v
);


OPTIMIZE TABLE dataos_explore.file_cache FINAL;

SELECT * FROM system.mutations;

CREATE OR REPLACE VIEW dataos_explore.content_v AS (
    SELECT
        id,
        created_at,
        body,
        content_timestamp,
        title,
        contributor,
        tags,
        languages,
        ngram_min_hash,
        ngram_hash,
        alpha_token_count,
        alpha_char_count,
        total_char_count,
        non_alpha_char_count,
        non_alpha_ratio,
        alpha_ratio,
        retrieved_at,
        'paragraph' AS app
    FROM dataos_explore.paragraph
    UNION ALL
    SELECT
        id,
        created_at,
        body,
        content_timestamp,
        title,
        contributor,
        tags,
        languages,
        ngram_min_hash,
        ngram_hash,
        alpha_token_count,
        alpha_char_count,
        total_char_count,
        non_alpha_char_count,
        non_alpha_ratio,
        alpha_ratio,
        retrieved_at,
        'lens' AS app
    FROM dataos_explore.lens_posts
    UNION ALL
    SELECT
        id,
        created_at,
        body,
        content_timestamp,
        title,
        contributor,
        [] AS tags,
        languages,
        ngram_min_hash,
        ngram_hash,
        alpha_token_count,
        alpha_char_count,
        total_char_count,
        non_alpha_char_count,
        non_alpha_ratio,
        alpha_ratio,
        retrieved_at,
        'mirrorxyz' AS app
    FROM dataos_explore.content_mirrorxyz_v2
    UNION ALL
    SELECT
        id,
        created_at,
        body,
        content_timestamp,
        title,
        contributor,
        [] AS tags,
        languages,
        ngram_min_hash,
        ngram_hash,
        alpha_token_count,
        alpha_char_count,
        total_char_count,
        non_alpha_char_count,
        non_alpha_ratio,
        alpha_ratio,
        retrieved_at,
        'cyberconnect' AS app
    FROM dataos_explore.cyberconnect
    UNION ALL
    SELECT
        id,
        created_at,
        body,
        content_timestamp,
        title,
        contributor,
        [] AS tags,
        languages,
        ngram_min_hash,
        ngram_hash,
        alpha_token_count,
        alpha_char_count,
        total_char_count,
        non_alpha_char_count,
        non_alpha_ratio,
        alpha_ratio,
        retrieved_at,
        'odysee' AS app
    FROM dataos_explore.odysee
);

SELECT count(1) FROM dataos_explore.content_v WHERE app = 'lens' AND alpha_token_count > 30 AND body NOT ILIKE '%i just voted%';
-- good cyberconnect contributor: readon_official

DROP TABLE IF EXISTS dataos_explore.quality_content;
CREATE TABLE dataos_explore.quality_content
ENGINE ReplacingMergeTree
ORDER BY (retrieved_at, created_at, id)
AS (
    SELECT DISTINCT ON (ngram_min_1) *, ngram_min_hash.1 AS ngram_min_1
    FROM dataos_explore.content_v
    WHERE
        non_alpha_ratio < 0.25 AND
        alpha_token_count > 30 AND
        languages['en'] > 0.9 AND
        CASE
            WHEN app = 'lens' THEN body NOT ILIKE '%i just voted%'
            WHEN app = 'cyberconnect' THEN alpha_token_count > 300
        ELSE TRUE
        END
);
SELECT COUNT(1) FROM dataos_explore.quality_content;

select * from dataos_explore.odysee;

DROP VIEW IF EXISTS  dataos_explore.quality_content_v;
CREATE VIEW dataos_explore.quality_content_v
AS (
    SELECT DISTINCT ON (ngram_min_1) *, ngram_min_hash.1 AS ngram_min_1
    FROM dataos_explore.content_v
    WHERE
        CASE WHEN {app:String} != 'any' THEN app = {app:String} ELSE TRUE END AND
        non_alpha_ratio < 0.25 AND
        alpha_token_count > 30 AND
        languages['en'] > 0.9 AND
        CASE
            WHEN app = 'lens' THEN body NOT ILIKE '%i just voted%'
            WHEN app = 'cyberconnect' THEN alpha_token_count > 300
        ELSE TRUE
        END
);


select * from dataos_explore.quality_content_v(app='odysee') limit 500;

SELECT
    *,
    multiMatchAllIndices(body, ['ethereum', 'staking']) AS matches
FROM dataos_explore.quality_content
WHERE length(matches) = 2 AND app = 'lens' ORDER BY alpha_token_count DESC;


SELECT * FROM default.transactions_silver_tbl WHERE tag_app_agg = 'twittar' LIMIT 10;

/*
 lens:
 -pK-AsnH91mktMC3He13OA7VNia7CwlB8pngKK1CUmE
 MsRmCMyvAsU-ZdHKYuqjmYKuF38axMHENexg5151HEk
 Ks_j1GqP5s8kBEvAd-wPvk0CeCptrvMj1xXLg89vdgw
 mirror:
 ZidHSpQjSc38mMr3pFnQvWqSqzIp7GRwZwTFwv7Ng-U
 0pq4kDxwiVU5HKY-YOITigJNExsDw5zYs0VA0GObooE
 OOMKXbK1D3DhJdPVnrDUjynDUUP4sD0-H7MByKayYew
 ekGfgVrZIOIS3ghxLVFPQVV8t842khQ_3W_lU_fSLWo
 cyberconnect:
 SX1EjgRnnfaCPoSscfNAEqslPTY_bSzyq5Qi173Hji4
 */


SELECT app, count(1)
FROM dataos_explore.content_v
    WHERE
        non_alpha_ratio < 0.25 AND
        alpha_token_count > 30 AND
        languages['en'] > 0.9 AND
        CASE
            WHEN app = 'lens' THEN body NOT ILIKE '%i just voted%'
            WHEN app = 'cyberconnect' THEN alpha_token_count > 300
        ELSE TRUE
        END
GROUP BY app;

SELECT app, count(1)
FROM dataos_explore.quality_content
GROUP BY app

OPTIMIZE TABLE dataos_explore.meta_eng_data_quality DEDUPLICATE BY retrieved_at, created_at, id;

SELECT * FROM dataos_explore.quality_content LIMIT 100;

SELECT count(1), uniq(ngram_min_hash.1)
    FROM dataos_explore.content_v
    WHERE
        non_alpha_ratio < 0.25 AND
        CASE WHEN app = 'lens' THEN body NOT ILIKE '%i just voted%' END AND
        CASE WHEN app = 'cyberconnect' THEN alpha_token_count > 300 ELSE alpha_token_count > 30 END
        AND languages['en'] > 0.9;

ORDER BY retrieved_at DESC LIMIT 500;

SELECT COUNT(1) FROM dataos_explore.indexable;
SELECT COUNT(1) FROM dataos_explore.file_cache;
SELECT * FROM system.query_log WHERE query LIKE '%file_cache%' ORDER BY initial_query_start_time DESC LIMIT 100;

ALTER USER default SETTINGS allow_experimental_nlp_functions = 1;

SELECT * FROM dataos_explore.content_v WHERE app = 'mirrorxyz' ORDER BY retrieved_at DESC  LIMIT 100;

SELECT * FROM dataos_explore.file_cache_tracker ORDER BY retrieved_at DESC  LIMIT 100;

SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;

SELECT JSON_VALUE(content_json, '$.title'), *
FROM (
    SELECT *, toString(JSONExtractString(content, 'content')) AS content_json
    FROM dataos_explore.file_cache
    WHERE id = 'Su7YUrtIukisMALnH_iBPuIcLxIhlTAQuNonYAw_yos'
);



CREATE TABLE dataos_explore.content (
    id String,
    author String,
    content_body String,
    title String,
    length Int32,
    languages Map,
    ngram_hash Tuple,
    app String,
    app_family String, -- eg lens
    source_type String --eg post, tweet, video, podcast
);



SELECT
    toStartOfMonth(created_at),
    count(1) transactions,
    count(DISTINCT owner_address) monthly_active_users
FROM transactions_silver_tbl GROUP BY 1;

WITH q AS (
    SELECT owner_address, count(1) as cnt
    FROM transactions_silver_tbl
    WHERE created_at >= '2022-01-01'
    GROUP BY owner_address
    ORDER BY cnt DESC
)
SELECT
    median(cnt) AS median_cnt,
    quantile(0.05)(cnt) AS bottom_5_percentile,
    quantile(0.10)(cnt) AS bottom_10_percentile,
    quantile(0.90)(cnt) AS top_10_percentile,
    quantile(0.95)(cnt) AS top_5_percentile
FROM q;

CREATE DATABASE dataos_explore;

SELECT COUNT(1) FROM dataos_explore.dataos_relevant_tx_mirror_paragraph;

SELECT * FROM dataos_explore.dataos_relevant_tx WHERE tag_app_agg = 'cyberconnect' LIMIT 100;

CREATE TABLE dataos_explore.dataos_relevant_tx
ENGINE=ReplacingMergeTree ORDER BY (id) AS
SELECT * FROM prod.transactions_silver_tbl WHERE tag_app_agg IN ('mirrorxyz', 'paragraph', 'cyberconnect', 'hey.xyz', 'lenster');

INSERT INTO dataos_explore.dataos_relevant_tx SELECT * FROM transactions_silver_tbl WHERE tag_app_agg IN ('lenstube', 'tape')


SELECT * FROM transactions_silver_tbl WHERE tag_app_agg IN ('lenstube', 'tape')

DROP TABLE IF EXISTS dataos_explore.dataos_relevant_tx_mirror_paragraph SYNC;
CREATE TABLE dataos_explore.dataos_relevant_tx_mirror_paragraph
ENGINE=ReplacingMergeTree
ORDER BY (id, cityHash64(id))
SAMPLE BY cityHash64(id)
AS
SELECT * FROM dataos_explore.dataos_relevant_tx WHERE tag_app_agg IN ('mirrorxyz', 'paragraph');


DROP TABLE IF EXISTS dataos_explore.dataos_relevant_tx_cyberconnect;
CREATE TABLE dataos_explore.dataos_relevant_tx_cyberconnect
ENGINE=ReplacingMergeTree ORDER BY (id) AS
SELECT * FROM dataos_explore.dataos_relevant_tx WHERE tag_app_agg IN ('cyberconnect');


DROP TABLE IF EXISTS dataos_explore.dataos_relevant_tx_lens SYNC;
CREATE TABLE dataos_explore.dataos_relevant_tx_lens
ENGINE=ReplacingMergeTree
ORDER BY (id, cityHash64(id))
SAMPLE BY cityHash64(id)
AS
SELECT *, case when tag_app_agg IN ('hey.xyz', 'lenster', 'lenstube', 'tape.xyz') then 'video' else 'text' end as lens_content_type FROM dataos_explore.dataos_relevant_tx WHERE tag_app_agg IN ('hey.xyz', 'lenster', 'lenstube', 'tape.xyz');


SELECT COUNT(1) FROM dataos_explore.dataos_relevant_tx WHERE  tag_app_agg IN ('hey.xyz', 'lenster');


WITH q AS (
    SELECT id
    FROM dataos_explore.dataos_relevant_tx_mirror_paragraph
    SAMPLE 10000
)
SELECT *
FROM q
LEFT JOIN dataos_explore.files f
USING (id) WHERE f.content = ''
LIMIT 500

CREATE TABLE dataos_explore.files (
    id String,
    content String,
    retrieved_at DateTime
)
ENGINE ReplacingMergeTree
ORDER BY id;

CREATE TABLE dataos_explore.files_lens (
    id String,
    content String,
    retrieved_at DateTime
)
ENGINE ReplacingMergeTree
ORDER BY id;


SELECT * FROM prod.transactions_silver_tbl WHERE tag_app_agg ILIKE 'Paragraph' LIMIT 100;
FROM dataos_explore.files
JOIN

SELECT
    files.id AS content_id,
    JSON_VALUE(files.content, '$.content.body') AS body
FROM dataos_explore.files
LIMIT 100;

DROP VIEW IF EXISTS dataos_explore.content_mirrorxyz;
CREATE VIEW dataos_explore.content_mirrorxyz AS
SELECT
    files.id AS content_id,
    JSON_VALUE(files.content, '$.content.body') AS content,
    JSON_VALUE(files.content, '$.content.timestamp') AS content_timestamp,
    JSON_VALUE(files.content, '$.content.title') AS title,
    JSON_VALUE(files.content, '$.digest') AS digest,
    JSON_VALUE(files.content, '$.authorship.contributor') AS contributor,
    JSON_VALUE(files.content, '$.authorship.signingKey') AS signing_key,
    JSON_VALUE(files.content, '$.authorship.signature') AS signature,
    JSON_VALUE(files.content, '$.authorship.signingKeySignature') AS signing_key_signature,
    JSON_VALUE(files.content, '$.authorship.signingKeyMessage') AS signing_key_message,
    JSON_VALUE(files.content, '$.authorship.algorithm.name') AS algorithm_name,
    JSON_VALUE(files.content, '$.authorship.algorithm.hash') AS algorithm_hash,
    JSON_VALUE(files.content, '$.version') AS version_date,
    JSON_VALUE(files.content, '$.wnft.chainId') AS wnft_chain_id,
    JSON_VALUE(files.content, '$.wnft.description') AS wnft_description,
    JSON_VALUE(files.content, '$.wnft.fundingRecipient') AS wnft_funding_recipient,
    JSON_VALUE(files.content, '$.wnft.imageURI') AS wnft_image_uri,
    JSON_VALUE(files.content, '$.wnft.mediaAssetId') AS wnft_media_asset_id,
    JSON_VALUE(files.content, '$.wnft.name') AS wnft_name,
    JSON_VALUE(files.content, '$.wnft.nonce') AS wnft_nonce,
    JSON_VALUE(files.content, '$.wnft.owner') AS wnft_owner,
    JSON_VALUE(files.content, '$.wnft.price') AS wnft_price,
    JSON_VALUE(files.content, '$.wnft.proxyAddress') AS wnft_proxy_address,
    JSON_VALUE(files.content, '$.wnft.renderer') AS wnft_renderer,
    JSON_VALUE(files.content, '$.wnft.supply') AS wnft_supply,
    JSON_VALUE(files.content, '$.wnft.symbol') AS wnft_symbol,
    JSON_VALUE(files.content, '$.wnft.hasCustomWnftMedia') AS wnft_has_custom_media
FROM dataos_explore.files
JOIN dataos_explore.dataos_relevant_tx_mirror_paragraph USING (id)
WHERE tag_app_agg = 'mirrorxyz';

OPTIMIZE TABLE dataos_explore.files FINAL;
OPTIMIZE TABLE dataos_explore.files_lens FINAL;

SELECT * FROM dataos_explore.files WHERE id = '39yB1LWIz37fdlvxmpB4B8PMaedqEo23uefmN1NZOtc';

ALTER TABLE dataos_explore.files DELETE WHERE retrieved_at > '2023-11-23';


WITH q AS (
    SELECT id
    FROM dataos_explore.indexable
    ANTI JOIN dataos_explore.files f USING (id)
    LIMIT 10000
)
SELECT DISTINCT id
FROM q
ORDER BY randCanonical()
LIMIT 100;

SELECT
    COUNT(1)
FROM dataos_explore.dataos_relevant_tx
;

SELECT id, f.id FROM dataos_explore.dataos_relevant_tx LEFT JOIN dataos_explore.files f USING (id) WHERE f.id = '' LIMIT 100;


SELECT created_at, get_app_tag(tags) FROM transactions WHERE id = 'YWqnA_KSIm_yaVky763SuAigX7gORhtvOFphr7ymljs'


SELECT * FROM prod.transactions_silver_tbl WHERE id = 'YWqnA_KSIm_yaVky763SuAigX7gORhtvOFphr7ymljs' AND toDate(created_at) = '2023-11-04';

SELECT tag_app_agg, count(1) FROM transactions_silver_tbl WHERE tag_app_agg ILIKE 'lenster%' GROUP BY 1;


WITH q AS (
    SELECT *
    FROM transactions_silver_tbl
    WHERE created_at BETWEEN '2023-03-01' AND '2023-05-01'
), agg AS (
    SELECT owner_address, count(1) cnt FROM q GROUP BY 1
)
-- SELECT cnt, count(1) FROM agg GROUP BY cnt ORDER BY 2 DESC;




-- SELECT count(distinct owner_address)
SELECT *
FROM q WHERE owner_address IN (SELECT owner_address FROM agg WHERE cnt = 24) AND tag_app_agg = '' AND get_tag('User-Agent', tags) = 'arkb'


SELECT COUNT(DISTINCT owner_address) FROM transactions_silver_tbl WHERE is_bundle;

WITH q AS (
SELECT table,
    sum(bytes) as size,
    min(min_date) as min_date,
    max(max_date) as max_date
    FROM system.parts
    WHERE active
GROUP BY table
ORDER BY size DESC)
SELECT *, formatReadableSize(size) FROM q;

DROP TABLE IF EXISTS transactions_silver_test;

SET allow_experimental_nlp_functions=1;
DROP TABLE IF EXISTS dataos_explore.content_mirrorxyz_mv;
CREATE TABLE dataos_explore.content_mirrorxyz_mv
ENGINE ReplacingMergeTree
ORDER BY content_id
AS
(
    SELECT
        *,
        wordShingleSimHashCaseInsensitive(content) as shingle_sim_hash,
        ngramMinHashCaseInsensitive(content) AS ngram_hash,
        hex(SHA1(content)) sha1_content_hash,
        detectLanguageMixed(content) AS languages
    FROM dataos_explore.content_mirrorxyz
);


SELECT COUNT(1), COUNT(DISTINCT sha1_content_hash), COUNT(DISTINCT shingle_sim_hash), COUNT(DISTINCT ngram_hash)  FROM dataos_explore.content_mirrorxyz_mv;

SELECT
    ngram_hash,
    count(1),
    max(length(content))
FROM dataos_explore.content_mirrorxyz_mv
GROUP BY 1
ORDER BY 2 DESC

SELECT * FROM dataos_explore.content_mirrorxyz_mv WHERE ngram_hash = (13404520679252436953, 17051435608730852809)

ALTER TABLE dataos_explore.content_mirrorxyz_mv ADD INDEX content_lowercase(lower(content)) TYPE inverted;
ALTER TABLE dataos_explore.content_mirrorxyz_mv MATERIALIZE INDEX content_lowercase;


SELECT * FROM dataos_explore.content_mirrorxyz_mv;
SELECT * FROM system.mutations;
SELECT * FROM system.processes;

SET allow_experimental_nlp_functions=1;
WITH q AS (
SELECT
    content_id,
    content,
    hex(sha1_content_hash),
    shingle_sim_hash,
    multiMatchAllIndices(content, ['Arweave', 'Redstone']) AS indices
FROM dataos_explore.content_mirrorxyz_mv
WHERE length(indices) > 0 AND length(content) > 100 --AND langs['en'] > 0.9
)
SELECT * FROM q
-- WHERE detectLanguageMixed(content)['en'] > 0.9
LIMIT 1000;

SELECT * FROM system.processes;


SELECT * FROM prod.transactions_silver_tbl WHERE tag_app_agg = 'hdbaas'



