select count(distinct id), 'keybert_keywords' from dataos_explore.meta_keybert_keywords
union all
select count(distinct id), 'meta_bart_categories' from dataos_explore.meta_bart_categories
union all
select count(distinct id), 'meta_text_embeddings' from dataos_explore.meta_text_embeddings
union all
select count(distinct id), 'meta_text_style' from dataos_explore.meta_text_style
union all
select count(distinct id), 'meta_text_summary' from dataos_explore.meta_text_summary
union all
select count(distinct id), 'meta_text_title' from dataos_explore.meta_text_title
union all
select count(distinct document_id), 'spacy_entities' from dataos_explore.spacy_entities
union all
select count(distinct document_id), 'flair_entities' from dataos_explore.flair_entities
union all
select count(distinct id), 'entities_metadata' from dataos_explore.entities_metadata
