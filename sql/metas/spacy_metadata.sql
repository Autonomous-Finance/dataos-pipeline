CREATE TABLE dataos_explore.spacy_entities
(
    id                    String,
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
ENGINE = ReplacingMergeTree ORDER BY (id, created_at)
    SETTINGS index_granularity = 8192;

