import os
import re

import spacy
import nltk
import spacy_transformers
from nltk.corpus import words as nltk_words
from thefuzz import fuzz
from thefuzz import process
import numpy as np


os.system("python3 -m spacy download en_core_web_sm --quiet")
os.system("python3 -m spacy download en_core_web_trf --quiet")


spacy_nlp_sm = spacy.load("en_core_web_sm")
# spacy_nlp_trf = spacy_nlp_sm
spacy_nlp_trf = spacy.load("en_core_web_trf")
nltk.download('words')


def clean_all(text: str) -> str:
    text = re.sub(r'http\S+', ' ', text)
    text = re.sub("#[A-Za-z0-9_]+", "", text)
    text = re.sub("[^0-9A-Za-z ]", "", text)
    text = text.replace('\n', '').replace('\t', '')
    text = re.sub(' +', ' ', text)
    text = text.lower()
    return text


def eng_quality_ratio(text: str) -> float:
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


ENT_TYPES = [
    'PERSON',
    'NORP',
    'FAC',
    'ORG',
    'GPE',
    'LOC',
    'PRODUCT',
    'EVENT',
    'WORK_OF_ART',
    'LAW',
    'LANGUAGE'
]


def extract_spacy_entities(text: str, ent_types=ENT_TYPES) -> dict[list]:
    entities = {}
    doc = spacy_nlp_trf(text)
    for ent in doc.ents:
        if ent.label_ in ent_types:
            if ent.label_ not in entities:
                entities[ent.label_] = []
            if ent.text not in entities[ent.label_]:
                entities[ent.label_].append(ent.text)

    return entities


def match_keywords(words_1: list[str], words_2: list[str], fuzz_treshold: int = 90) -> int:
    num_matches = 0
    for word_1 in words_1:
        for word_2 in words_2:
            if fuzz.ratio(word_1, word_2) > fuzz_treshold:
                num_matches += 1
    return num_matches


def cosine_similarity(a, b) -> float:
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
