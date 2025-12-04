from datetime import datetime
import os

from prefect import task, flow
from prefect import serve
from prefect.server.schemas.schedules import CronSchedule
import pandas as pd
from tenacity import retry
import tenacity

from flows.util.clickhouse import get_ch_client
from flows.util.openai import generate_text_style, generate_text_title,\
    generate_text_summary, get_embeddings


@task
def get_documents_to_process(num_records=1000, app='any'):

    query = f"""
    select se.document_id as id,
           cv.body as content
    from dataos_explore.quality_content_v ('{app}') qc
    anti join dataos_explore.meta_text_embeddings te ON qc.id = te.id
    limit {num_records}
    """

    ch_client = get_ch_client()
    result_df = ch_client.query_df(query)
    print(f'number of documents: {len(result_df)}')
    return result_df


@task
def gen_gpt_text_embeddings(df: pd.DataFrame):
    @retry(
        wait=tenacity.wait_fixed(10),
        stop=tenacity.stop_after_attempt(5)
    )
    def _get_embeddings(*args):
        return get_embeddings(*args)

    df['embedding'] = _get_embeddings([x[:15000] for x in list(df['content'].values)])
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
        wait=tenacity.wait_fixed(10),
        stop=tenacity.stop_after_attempt(5)
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
        wait=tenacity.wait_fixed(10),
        stop=tenacity.stop_after_attempt(5)
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
        wait=tenacity.wait_fixed(10),
        stop=tenacity.stop_after_attempt(5)
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
def generate_gpt_metadata_flow(num_records=2000):
    df = get_documents_to_process(num_records=num_records)
    gen_gpt_text_embeddings(df[['id', 'content']])
    # gen_text_summary(df)
    # gen_text_title(df)
    # gen_text_style(df)


if __name__ == "__main__":
    generate_metadata_deploy = generate_gpt_metadata_flow.to_deployment(
        name=f"{os.environ['DEPLOYMENT_NAME']}",
        schedule=(CronSchedule(cron="*/15 * * * *"))
    )
    serve(generate_metadata_deploy)
