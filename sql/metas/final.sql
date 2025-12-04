CREATE OR REPLACE VIEW dataos_explore.odysee_meta AS (
SELECT
    qc.id AS id,
    qc.created_at AS created_at,
    qc.body AS body,
    qc.content_timestamp AS content_timestamp,
    qc.title AS title,
    qc.contributor AS contributor,
    toJSONString(qc.tags) AS tags,
    toJSONString(qc.languages) AS languages,
    toJSONString(qc.ngram_min_hash) AS ngram_min_hash,
    qc.ngram_hash AS ngram_hash,
    qc.alpha_token_count AS alpha_token_count,
    qc.alpha_char_count AS alpha_char_count,
    qc.total_char_count AS total_char_count,
    qc.non_alpha_char_count AS non_alpha_char_count,
    qc.non_alpha_ratio AS non_alpha_ratio,
    qc.alpha_ratio AS alpha_ratio,
    qc.retrieved_at AS retrieved_at,
    qc.app AS app,
    qc.ngram_min_1 AS ngram_min_1,
    concat(se.person_entities, ',', fe.person_entities) as person_entities,
    concat(se.norp_entities, ',', fe.norp_entities) as norp_entities,
    concat(se.fac_entities, ',', fe.fac_entities) as fac_entities,
    concat(se.org_entities, ',', fe.org_entities) as org_entities,
    concat(se.gpe_entities, ',', fe.gpe_entities) as gpe_entities,
    concat(se.loc_entities, ',', fe.loc_entities) as loc_entities,
    concat(se.product_entities, ',', fe.product_entities) as product_entities,
    concat(se.event_entities, ',', fe.event_entities) as event_entities,
    concat(se.work_of_art_entities, ',', fe.work_of_art_entities) as work_of_art_entities,
    concat(se.law_entities, ',', fe.law_entities) as law_entities,
    fe.date_entities as date_entities,
    c.top_categories as top_category,
    toJSONString(c.categories) as categories,
    kw.keywords as keywords
FROM dataos_explore.quality_content_v(app='odysee') qc
LEFT JOIN dataos_explore.flair_entities fe ON fe.id = qc.id
LEFT JOIN dataos_explore.spacy_entities se ON se.id = qc.id
LEFT JOIN dataos_explore.meta_keybert_keywords kw ON kw.id = qc.id
LEFT JOIN dataos_explore.meta_bart_categories c ON c.id = qc.id
);

select * from dataos_explore.odysee_meta