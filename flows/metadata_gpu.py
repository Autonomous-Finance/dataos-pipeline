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
        select body as content, id from dataos_explore.content_v t
        where t.id NOT IN (SELECT document_id FROM dataos_explore.spacy_entities)
        and t.id NOT IN (SELECT document_id FROM dataos_explore.flair_entities)
        and languages['en'] > 0.5
        and length(body) > 70
        and length(splitByChar(' ', t.body)) > 10
        limit {num_records}
      """

    client = get_ch_client()
    result = client.query(query)

    rows = result.result_rows
    texts_df = pd.DataFrame({"text": [r[0] for r in rows], 'id': [r[1] for r in rows]})
    print(f'number of intial documents: {len(texts_df)}')
    clean_texts = []

    for idx, row in texts_df.iterrows():
        text = row['text']
        clean_texts.append({'id': row['id'], 'text': text, 'quality_ratio': None})

    print('number of quality texts:', len(clean_texts))
    return clean_texts


@task
def generate_spacy_entities(texts: list[dict]):
    for n in range(len(texts)):
        text = texts[n]['text']
        entities = extract_spacy_entities(text)
        texts[n]['entities'] = entities

    clean_texts_df = pd.DataFrame(texts)
    if len(clean_texts_df) == 0:
        print('no records to save')

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
    ts = datetime.now()
    for idx, row in clean_texts_df.iterrows():
        row_ = {}
        for column in column_names[2:]:
            name = column[:-9].upper()
            if name in row['entities']:
                value = ','.join([x.replace(',', '') for x in row['entities'][name]])
            else:
                value = ''
            row_[f'{name.lower()}_entities'] = value
            row_['document_id'] = row['id']
            row_['created_at'] = ts

        res_list.append(row_)

    res_df = pd.DataFrame(res_list)
    print(f'saving records: {res_df.shape}')
    rows = list(res_df[column_names].itertuples(index=False, name=None))

    client = get_ch_client()
    client.insert(
        'dataos_explore.spacy_entities',
        rows,
        column_names=column_names
    )


@task
def generate_flair_entities(texts: list[dict]):

    for n in range(len(texts)):
        text = texts[n]['text']
        entities = extract_flair_18c_entities(text)
        texts[n]['entities'] = entities

    clean_texts_df = pd.DataFrame(texts)
    if len(clean_texts_df) == 0:
        raise ValueError('no records to save')

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
    ts = datetime.now()
    for idx, row in clean_texts_df.iterrows():
        row_ = {}
        for column in column_names[2:]:
            name = column[:-9].upper()
            if name in row['entities']:
                value = ','.join([x.replace(',', '') for x in row['entities'][name]])
            else:
                value = ''
            row_[f'{name.lower()}_entities'] = value
            row_['document_id'] = row['id']
            row_['created_at'] = ts

        res_list.append(row_)

    res_df = pd.DataFrame(res_list)
    print(f'saving records: {res_df.shape}')
    rows = list(res_df[column_names].itertuples(index=False, name=None))

    client = get_ch_client()
    client.insert(
        'dataos_explore.flair_entities',
        rows,
        column_names=column_names
    )


@task
def generate_entity_metadata(num_records=1000):

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
    from dataos_explore.spacy_entities as se
    right join dataos_explore.flair_entities as fe
    on se.document_id = fe.document_id
    where se.document_id not in (select document_id from dataos_explore.entities_metadata)
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

    ch_client.insert_df(
        table='entities_metadata',
        df=result_df,
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
    texts = load_data(num_records=num_records)
    generate_spacy_entities(texts)
    generate_flair_entities(texts)

    df = pd.DataFrame({'id': [x['id'] for x in texts], 'content': [x['text'] for x in texts]})
    generate_keybert_kwords(df)
    generate_bart_categories(df)


if __name__ == "__main__":
    generate_metadata_deploy = generate_metadata_task.to_deployment(
        name=f"{os.environ['DEPLOYMENT_NAME']}"
    )
    serve(generate_metadata_deploy)
