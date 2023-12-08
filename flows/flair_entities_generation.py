from datetime import datetime
import os

from prefect import task, flow
from prefect import serve
import pandas as pd

from util.clickhouse import get_ch_client
from util.common import load_env
from util.nlp import eng_quality_ratio
from util.flair_ner import extract_flair_18c_entities


text_max_chars = 100000


@task
def generate_flair_entities(num_records=300):

    target_quality_ratio = 0.3
    ts = datetime.now()

    query = f"""
    SELECT t.content, content_id
    FROM dataos_explore.content_mirrorxyz t
    where t.content_id NOT IN (SELECT document_id FROM dataos_explore.flair_entities)
    and t.content_id NOT IN (SELECT document_id FROM dataos_explore.eng_data_quality where quality_score < {target_quality_ratio} )
    and length(content) > 70
    and length(splitByChar(' ', t.content)) > 10
    limit {num_records}
    """

    client = get_ch_client()
    result = client.query(query)

    rows = result.result_rows
    texts_df = pd.DataFrame({"text": [r[0] for r in rows], 'id': [r[1] for r in rows]})
    print(f'number of intial documents: {len(texts_df)}')
    clean_texts = []
    dq_res = []

    for idx, row in texts_df.iterrows():
        text = row['text']
        if len(text) > text_max_chars:
            text_ = text[:text_max_chars]
        else:
            text_ = text
        quality_ratio = eng_quality_ratio(text_)

        if quality_ratio < target_quality_ratio:
            clean_texts.append({'id': row['id'], 'text': text, 'quality_ratio': quality_ratio})

        dq_res.append({'document_id': row['id'], 'quality_score': quality_ratio, 'created_at': ts})

    dq_df = pd.DataFrame(dq_res)
    dq_columns = [
        'document_id',
        'quality_score',
        'created_at'
    ]
    dq_rows = list(dq_df[dq_columns].itertuples(index=False, name=None))
    client.insert(
        'dataos_explore.eng_data_quality',
        dq_rows,
        column_names=dq_columns
    )

    print('number of quality texts:', len(clean_texts))

    for n in range(len(clean_texts)):
        text = clean_texts[n]['text']
        entities = extract_flair_18c_entities(text)
        clean_texts[n]['entities'] = entities

    clean_texts_df = pd.DataFrame(clean_texts)
    if len(clean_texts_df) == 0:
        print('no records to save')
        return

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

    client.insert(
        'dataos_explore.flair_entities',
        rows,
        column_names=column_names
    )


@flow(name="Generate flair entities", log_prints=True)
def generate_entities():
    generate_flair_entities()


if __name__ == "__main__":
    load_env()
    generate_entities_deploy = generate_entities.to_deployment(
        name=f"{os.environ['DEPLOYMENT_NAME']}-flair-entities",
        cron="*/45 * * * *"
    )
    serve(generate_entities_deploy)
