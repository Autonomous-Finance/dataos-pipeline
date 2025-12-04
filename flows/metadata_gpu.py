from datetime import datetime
import os

from prefect import task, flow
from prefect import serve
import pandas as pd
from typing import List, Dict, Optional

from flows.util.clickhouse import get_ch_client
from flows.util.bart_categories import flatten_bart_categories, gen_bart_categories
from flows.util.keybert import gen_keybert_kwords
from flows.util.nlp import extract_spacy_entities
from flows.util.flair_ner import extract_flair_18c_entities


@task
def load_data(num_records=1000, app: str='any') -> List[Dict]:
    query = f"""
        select 
            body as content, 
            t.id as id,
            s.id != '' as has_spacy,
            f.id != '' as has_flair,
            bc.id != '' as has_bart_categories,
            kw.id != '' as has_keybert_keywords
        from dataos_explore.quality_content_v (app='{app}') t
        left join dataos_explore.spacy_entities s ON t.id = s.id
        left join dataos_explore.flair_entities f ON t.id = f.id
        left join dataos_explore.meta_bart_categories bc ON t.id = bc.id
        left join dataos_explore.meta_keybert_keywords kw ON t.id = kw.id
        where least(has_spacy, has_flair, has_bart_categories, has_keybert_keywords) = 0
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
        'id',
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
            row_['id'] = row['id']
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
        'id',
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
            row_['id'] = row['id']
        res_list.append(row_)

    res_df = pd.DataFrame(res_list)
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

@task
def check_gpu():
    import torch
    print("torch gpu check", torch.cuda.is_available())

def exec_conditionally(fn: callable, df: pd.DataFrame):
    if not df.empty:
        print(str(fn))
        print(df)
        return fn(df)
@flow(name="Generate metadata on gpu", log_prints=True)
def generate_gpu_metadata_flow(num_records=100, app: str='any'):
    check_gpu()
    df = load_data(num_records=num_records, app=app)

    print(df)
    if not df.empty:
        exec_conditionally(generate_spacy_entities, df[df['has_spacy'] == 0])
        exec_conditionally(generate_flair_entities, df[df['has_flair'] == 0])
        exec_conditionally(generate_keybert_kwords, df[df['has_keybert_keywords'] == 0])
        exec_conditionally(generate_bart_categories, df[df['has_bart_categories'] == 0])
    else:
        print('nothing to process')

if __name__ == "__main__":
    generate_metadata_deploy = generate_metadata_flow.to_deployment(
        name=f"{os.environ['DEPLOYMENT_NAME']}"
    )
    serve(generate_metadata_deploy)
