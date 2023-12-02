import os
from datetime import datetime

from prefect import task, flow
from prefect import serve
import pandas as pd

from util.clickhouse import get_ch_client
from util.common import load_env
from util.nlp import eng_quality_ratio, extract_spacy_entities


text_max_chars = 100000


@task
def generate_spacy_entities():

    query = """
    SELECT t.content, content_id
    FROM dataos_explore.content_mirrorxyz t
    where t.content_id NOT IN (SELECT document_id FROM dataos_explore.spacy_entities)
    and length(content) > 70
    and length(splitByChar(' ', t.content)) > 10
    limit 200
    """

    client = get_ch_client()
    result = client.query(query)

    rows = result.result_rows
    texts_df = pd.DataFrame({"text": [r[0] for r in rows], 'id': [r[1] for r in rows]})
    clean_texts = []

    for idx, row in texts_df.iterrows():
        text = row['text']
        if len(text) > text_max_chars:
            text_ = text[:text_max_chars]
        else:
            text_ = text
        quality_ratio = eng_quality_ratio(text_)

        if quality_ratio < 0.3:
            continue

        clean_texts.append({'id': row['id'], 'text': text, 'quality_ratio': quality_ratio})

    print('number of quality texts:', len(clean_texts))

    for n in range(len(clean_texts)):
        text = clean_texts[n]['text']
        entities = extract_spacy_entities(text)
        clean_texts[n]['entities'] = entities

    clean_texts_df = pd.DataFrame(clean_texts)

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
    rows = list(res_df[column_names].itertuples(index=False, name=None))

    client.insert(
        'dataos_explore.spacy_entities',
        rows,
        column_names=column_names
    )


@flow(name="Generate spacy entities", log_prints=True)
def generate_entities():
    generate_spacy_entities()


if __name__ == "__main__":
    load_env()
    generate_entities_deploy = generate_entities.to_deployment(name=os.environ['DEPLOYMENT_NAME'])
    serve(generate_entities_deploy)
