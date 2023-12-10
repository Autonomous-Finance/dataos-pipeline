from prefect import flow, task
import spacy
import nltk
from nltk.corpus import words as nltk_words
import pandas as pd
import re
from flows.util.clickhouse import get_ch_client
from datetime import datetime
from typing import Tuple, List, Dict
import os

TARGET_QUALITY_RATIO = 0.3
TEXT_MAX_CHARS = 100000
client = get_ch_client()
nltk.download('words')

@task
def get_documents(from_retrieved_at: datetime, from_created_at: datetime, limit: int = 100) -> pd.DataFrame:

    query = f"""
    WITH q AS (
        SELECT id, retrieved_at, created_at, body
        FROM dataos_explore.quality_content q
        ANTI JOIN dataos_explore.meta_eng_data_quality USING (id)
        LIMIT 10000
    )
    SELECT DISTINCT ON (id) id, retrieved_at, created_at, body
    FROM q
    ORDER BY randCanonical()
    LIMIT {limit};
    """
    df_docs = client.query_df(query)
    df_docs['content_short'] = df_docs['body'].str.slice(0, TEXT_MAX_CHARS)
    df_docs['processed_at'] = pd.Timestamp.utcnow()

    return df_docs


def clean_all(text: str) -> str:
    text1 = re.sub(r'http\S+', ' ', text)
    text2 = re.sub("#[A-Za-z0-9_]+", "", text1)
    text3 = re.sub("[^0-9A-Za-z ]", "", text2)
    text4 = text3.replace('\n', '').replace('\t', '')
    text5 = re.sub(' +', ' ', text4)
    text6 = text5.lower()
    return text6


def get_english_quality_score(text: str, spacy_nlp_sm) -> float:
    original_len = len(text)
    if original_len == 0:
        return 0.0

    text = clean_all(text.lower())
    doc = spacy_nlp_sm(text)

    valid_words = []

    for token in doc:
        if token.lemma_ in nltk_words.words():
            valid_words.append(token.text)

    quality_ratio = len(' '.join(valid_words)) / original_len
    return quality_ratio

@task
def assess_quality(df: pd.DataFrame) -> pd.DataFrame:
    spacy_nlp_sm = spacy.load("en_core_web_sm")
    df['eng_quality'] = df['content_short'].map(lambda content_short: get_english_quality_score(content_short, spacy_nlp_sm))
    return df


@flow
def check_text_quality():
    df_docs = get_documents(None, None)
    df_results = assess_quality(df_docs)
    client.insert_df(
        table='meta_eng_data_quality',
        df=df_results.drop("body", axis=1),
        database='dataos_explore'
    )


if __name__ == "__main__":
    # Create and serve the deployment
    check_text_quality.serve(name="my-first-deployment")
