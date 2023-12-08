from datetime import datetime
import os

from prefect import task, flow
from prefect import serve
import pandas as pd

from util.clickhouse import get_ch_client
from util.common import load_env

load_env()

from util.bart_categories import flatten_bart_categories, gen_bart_categories
from util.keybert import gen_keybert_kwords
from util.openai import generate_text_style, generate_text_title, generate_text_summary, get_embeddings


@task
def generate_metadata(num_records=20):

    query = f"""
    select se.document_id as document_id,
           cv.body as content,
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
           fe.language_entities as language_entities,
           fe.money_entities as money_entities,
           fe.quantity_entities as quantity_entities,
           fe.time_entities as time_entities,
           cv.ngram_hash as content_hash,
           cv.languages as languages,
           cv.alpha_token_count as alpha_token_count,
           cv.alpha_char_count as alpha_char_count,
           cv.total_char_count as total_char_count,
           cv.non_alpha_char_count as non_alpha_char_count,
           cv.non_alpha_ratio as non_alpha_ratio,
           cv.alpha_ratio as alpha_ratio
    from dataos_explore.spacy_entities as se
    right join dataos_explore.flair_entities as fe
    on se.document_id = fe.document_id
    right join dataos_explore.content_v as cv on se.document_id = cv.id
    where se.document_id not in (select document_id from dataos_explore.documents_metadata)
    limit {num_records}
    """

    ch_client = get_ch_client()
    result_df = ch_client.query_df(query)

    result_df['created_ts'] = datetime.now()
    result_df['document_type'] = 'text'

    def process_tags(text):
        res = [x for x in text.split(',') if len(x) != 0]
        return ','.join(res)

    process_tag_columns = [
        'person_entities',
        'norp_entities',
        'fac_entities',
        'org_entities',
        'gpe_entities',
        'loc_entities',
        'product_entities',
        'event_entities',
        'work_of_art_entities',
        'law_entities'
    ]

    for col in process_tag_columns:
        result_df[col] = result_df[col].apply(process_tags)

    for col in [
        'person_entities',
        'norp_entities',
        'fac_entities',
        'org_entities',
        'gpe_entities',
        'loc_entities',
        'product_entities',
        'event_entities',
        'work_of_art_entities',
        'law_entities',
        'date_entities',
        'language_entities',
        'money_entities',
        'quantity_entities',
        'time_entities',
    ]:
        result_df[col] = result_df[col].apply(lambda x: ','.join(list(set(x.split(',')))))

    result_df['text_embedding'] = get_embeddings(list(result_df['content'].values))
    result_df['content_categories'] = result_df['content'].apply(gen_bart_categories)

    result_df['text_summary'] = result_df['content'].apply(generate_text_summary)
    result_df['text_title'] = result_df['content'].apply(generate_text_title)
    result_df['text_style'] = result_df['content'].apply(generate_text_style)

    result_df['languages'] = result_df['languages'].apply(lambda x: {k: float(v) for k, v in x.items()})

    result_df['content_top_categories'] = result_df['content_categories'].apply(
        lambda x: ','.join(flatten_bart_categories(x)))
    result_df['keywords'] = result_df['content_categories'].apply(lambda x: ','.join(gen_keybert_kwords(x)))

    columns = list(result_df.columns)
    rows = list(result_df[columns].itertuples(index=False, name=None))
    ch_client.insert(
        'dataos_explore.documents_metadata',
        rows,
        column_names=columns
    )


@flow(name="Generate all metadata", log_prints=True)
def generate_metadata_task():
    generate_metadata()


if __name__ == "__main__":
    load_env()
    generate_metadata_deploy = generate_metadata_task.to_deployment(
        name=f"{os.environ['DEPLOYMENT_NAME']}"
    )
    serve(generate_metadata_deploy)
