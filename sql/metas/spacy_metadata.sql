
create table dataos_explore.spacy_entities
(
    document_id          String,
    person_entities      String,
    norp_entities        String,
    fac_entities         String,
    org_entities         String,
    gpe_entities         String,
    loc_entities         String,
    product_entities     String,
    event_entities       String,
    work_of_art_entities String,
    law_entities         String,
    language_entities    String,
    created_at           DateTime
)
    engine = ReplacingMergeTree ORDER BY (created_at, document_id)
        SETTINGS index_granularity = 8192;
