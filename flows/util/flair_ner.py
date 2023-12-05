from flair.data import Sentence
from flair.models import SequenceTagger


# tagger = SequenceTagger.load("flair/ner-english-large")
# tagger = SequenceTagger.load("flair/ner-english")
# tagger = SequenceTagger.load("flair/ner-english-fast")
# tagger = SequenceTagger.load("flair/ner-english-ontonotes")
tagger = SequenceTagger.load("flair/ner-english-ontonotes-fast")


### Entity types ###
# CARDINAL	cardinal value
# DATE	date value
# EVENT	event name
# FAC	building name
# GPE	geo-political entity
# LANGUAGE	language name
# LAW	law name
# LOC	location name
# MONEY	money name
# NORP	affiliation
# ORDINAL	ordinal value
# ORG	organization name
# PERCENT	percent value
# PERSON	person name
# PRODUCT	product name
# QUANTITY	quantity value
# TIME	time value
# WORK_OF_ART	name of work of art


ENT_TYPES = (
  "CARDINAL",
  "DATE",
  "EVENT",
  "FAC",
  "GPE",
  "LANGUAGE",
  "LAW",
  "LOC",
  "MONEY",
  "NORP",
  "ORDINAL",
  "ORG",
  "PERCENT",
  "PERSON",
  "PRODUCT",
  "QUANTITY",
  "TIME",
  "WORK_OF_ART"
)


def extract_flair_18c_entities(text: str, ent_types=ENT_TYPES) -> dict[list]:
    res = {}
    for et in ent_types:
        res[et] = []

    sentence = Sentence(text)
    tagger.predict(sentence)

    for entity in sentence.get_spans('ner'):
        if entity.tag in ent_types:
            res[entity.tag].append(entity.text)

    for et in ent_types:
        res[et] = list(set(res[et]))

    return res
