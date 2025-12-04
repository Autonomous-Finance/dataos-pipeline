from functools import lru_cache
from typing import List, Dict

from transformers import pipeline
import torch


@lru_cache
def get_bart_classifier():
    return pipeline(
        "zero-shot-classification",
        model="facebook/bart-large-mnli",
        device=torch.device("cuda")
    )


candidate_labels = [
  'Pop Culture',
  'Artists',
  'Education',
  'Comedy',
  'Lifestyle',
  'Music',
  'Sports',
  'Gaming',
  'Tech',
  'Finance',
  'Spirituality',
  'News & Politics',
  'Universe',
  'Rabbit Hole'
]
candidate_labels = [c.lower() for c in candidate_labels]


def gen_bart_categories(text: str) -> Dict:
    bart_classifier = get_bart_classifier()
    res = bart_classifier(text, candidate_labels, multi_label=True)
    return dict(zip(res['labels'], res['scores']))


def flatten_bart_categories(data: Dict, treshold=0.7) -> List[str]:
    res = []
    for k, v in data.items():
        if v > treshold:
            res.append(k)
        return res
