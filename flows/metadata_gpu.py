from datetime import datetime
import os

from prefect import task, flow
from prefect import serve
import pandas as pd

from flows.util.clickhouse import get_ch_client
from flows.util.bart_categories import flatten_bart_categories, gen_bart_categories
from flows.util.keybert import gen_keybert_kwords
from util.nlp import extract_spacy_entities
from flows.util.flair_ner import extract_flair_18c_entities


@task
def load_data(num_records=1000) -> list[dict]:
    query = f"""
        select body as content, id from dataos_explore.quality_content t
        where t.id NOT IN (SELECT document_id FROM dataos_explore.spacy_entities)
        and t.id NOT IN (SELECT document_id FROM dataos_explore.flair_entities)
        limit {num_records}
      """

    ch_client = get_ch_client()
    result_df = ch_client.query_df(query)
    print(f'number of initial documents: {len(result_df)}')
    result_df['created_at'] = datetime.now()
    return result_df


@task
def generate_spacy_entities(texts_df: pd.DataFrame):
    texts_df['entities'] = texts_df['content'].apply(extract_spacy_entities)

    column_names = [
        'document_id',
        'created_at',
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
        'language_entities',
    ]

    res_list = []
    for idx, row in texts_df.iterrows():
        row_ = {}
        for column in column_names[2:]:
            name = column[:-9].upper()
            if name in row['entities']:
                value = ','.join([x.replace(',', '') for x in row['entities'][name]])
            else:
                value = ''
            row_[f'{name.lower()}_entities'] = value
            row_['document_id'] = row['id']
        res_list.append(row_)

    res_df = pd.DataFrame(res_list)
    res_df['created_at'] = datetime.now()
    print(f'saving records: {res_df.shape}')
    ch_client = get_ch_client()
    ch_client.insert_df(
        table='spacy_entities',
        df=res_df[column_names],
        database='dataos_explore'
    )


@task
def generate_flair_entities(texts_df: pd.DataFrame):
    texts_df['entities'] = texts_df['content'].apply(extract_flair_18c_entities)

    column_names = [
        'document_id',
        'created_at',
        'cardinal_entities',
        'date_entities',
        'event_entities',
        'fac_entities',
        'gpe_entities',
        'language_entities',
        'law_entities',
        'loc_entities',
        'money_entities',
        'norp_entities',
        'ordinal_entities',
        'org_entities',
        'percent_entities',
        'person_entities',
        'product_entities',
        'quantity_entities',
        'time_entities',
        'work_of_art_entities',
    ]

    res_list = []
    for idx, row in texts_df.iterrows():
        row_ = {}
        for column in column_names[2:]:
            name = column[:-9].upper()
            if name in row['entities']:
                value = ','.join([x.replace(',', '') for x in row['entities'][name]])
            else:
                value = ''
            row_[f'{name.lower()}_entities'] = value
            row_['document_id'] = row['id']
        res_list.append(row_)

    res_df = pd.DataFrame(res_list)
    res_df = res_df.rename(columns={'id': 'document_id'})
    res_df['created_at'] = datetime.now()
    print(f'saving records: {res_df.shape}')
    ch_client = get_ch_client()
    ch_client.insert_df(
        table='flair_entities',
        df=res_df[column_names],
        database='dataos_explore'
    )


@task
def generate_bart_categories(df: pd.DataFrame):
    df['categories'] = df['content'].apply(gen_bart_categories)
    df['top_categories'] = df['categories'].apply(
        lambda x: ','.join(flatten_bart_categories(x)))
    df['created_at'] = datetime.now()
    ch_client = get_ch_client()
    ch_client.insert_df(
        table='meta_bart_categories',
        df=df[['id', 'created_at', 'categories', 'top_categories']],
        database='dataos_explore'
    )


@task
def generate_keybert_kwords(df: pd.DataFrame):
    df['keywords'] = df['content'].apply(lambda x: ','.join(gen_keybert_kwords(x)))
    df['created_at'] = datetime.now()
    ch_client = get_ch_client()
    ch_client.insert_df(
        table='meta_keybert_keywords',
        df=df[['id', 'keywords', 'created_at']],
        database='dataos_explore'
    )


@flow(name="Generate metadata on gpu", log_prints=True)
def generate_metadata_task(num_records=100):
    df = load_data(num_records=num_records)
    generate_spacy_entities(df)
    generate_flair_entities(df)
    generate_keybert_kwords(df)
    generate_bart_categories(df)


if __name__ == "__main__":
    generate_metadata_deploy = generate_metadata_task.to_deployment(
        name=f"{os.environ['DEPLOYMENT_NAME']}"
    )
    serve(generate_metadata_deploy)
