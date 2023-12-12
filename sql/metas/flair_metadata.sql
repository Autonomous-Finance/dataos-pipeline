create table dataos_explore.flair_entities
(
    document_id          String,
    cardinal_entities    String,
    date_entities        String,
    event_entities       String,
    fac_entities         String,
    gpe_entities         String,
    language_entities    String,
    law_entities         String,
    loc_entities         String,
    money_entities       String,
    norp_entities        String,
    ordinal_entities     String,
    org_entities         String,
    percent_entities     String,
    person_entities      String,
    product_entities     String,
    quantity_entities    String,
    time_entities        String,
    work_of_art_entities String,
    created_at           DateTime
)
    engine = ReplacingMergeTree ORDER BY (created_at, document_id)
        SETTINGS index_granularity = 8192;
