from datetime import datetime
import os

from prefect import task, flow
from prefect import serve
import pandas as pd
from tenacity import retry
import tenacity

from flows.util.clickhouse import get_ch_client
from flows.util.openai import generate_text_style, generate_text_title,\
    generate_text_summary, get_embeddings


@task
def generate_entity_metadata(num_records=1000):

    query = f"""
    select se.document_id as id,
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
           fe.time_entities as time_entities
    from dataos_explore.spacy_entities as se
    right join dataos_explore.flair_entities as fe
    on se.document_id = fe.document_id
    right join dataos_explore.content_v as cv
    on se.document_id = cv.id
    where se.document_id not in (select id from dataos_explore.entities_metadata)
    limit {num_records}
    """

    ch_client = get_ch_client()
    result_df = ch_client.query_df(query)
    result_df['created_at'] = datetime.now()

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

    save_columns = [
        'id',
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
        'created_at',
    ]

    ch_client.insert_df(
        table='entities_metadata',
        df=result_df[save_columns],
        database='dataos_explore'
    )
    return result_df


@task
def gen_gpt_text_embeddings(df: pd.DataFrame):
    @retry(
        wait=tenacity.wait_fixed(1),
        stop=tenacity.stop_after_attempt(3)
    )
    def _get_embeddings(*args):
        return get_embeddings(*args)

    df['embedding'] = _get_embeddings(list(df['content'].values))
    df['created_at'] = datetime.now()
    ch_client = get_ch_client()
    ch_client.insert_df(
        table='meta_text_embeddings',
        df=df[['id', 'embedding', 'created_at']],
        database='dataos_explore'
    )


@task
def gen_text_summary(df: pd.DataFrame):
    @retry(
        wait=tenacity.wait_fixed(1),
        stop=tenacity.stop_after_attempt(3)
    )
    def _generate_text_summary(*args):
        return generate_text_summary(*args)

    df['summary'] = df['content'].apply(_generate_text_summary)
    df['created_at'] = datetime.now()
    ch_client = get_ch_client()
    ch_client.insert_df(
        table='meta_text_summary',
        df=df[['id', 'summary', 'created_at']],
        database='dataos_explore'
    )


@task
def gen_text_title(df: pd.DataFrame):
    @retry(
        wait=tenacity.wait_fixed(1),
        stop=tenacity.stop_after_attempt(3)
    )
    def _generate_text_title(*args):
        return generate_text_title(*args)

    df['title'] = df['content'].apply(_generate_text_title)
    df['created_at'] = datetime.now()
    ch_client = get_ch_client()
    ch_client.insert_df(
        table='meta_text_title',
        df=df[['id', 'title', 'created_at']],
        database='dataos_explore'
    )


@task
def gen_text_style(df: pd.DataFrame):
    @retry(
        wait=tenacity.wait_fixed(1),
        stop=tenacity.stop_after_attempt(3)
    )
    def _generate_text_style(*args):
        return generate_text_style(*args)

    df['style'] = df['content'].apply(generate_text_style)
    df['created_at'] = datetime.now()
    ch_client = get_ch_client()
    ch_client.insert_df(
        table='meta_text_style',
        df=df[['id', 'style', 'created_at']],
        database='dataos_explore'
    )


@flow(name="Generate metadata on cpu", log_prints=True)
def generate_metadata_task(num_records=10):
    df = generate_entity_metadata(num_records=num_records)
    gen_gpt_text_embeddings(df[['id', 'content']])
    gen_text_summary(df)
    gen_text_title(df)
    gen_text_style(df)


if __name__ == "__main__":
    generate_metadata_deploy = generate_metadata_task.to_deployment(
        name=f"{os.environ['DEPLOYMENT_NAME']}"
    )
    serve(generate_metadata_deploy)
