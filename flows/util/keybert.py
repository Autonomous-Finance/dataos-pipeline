from typing import List

from keybert import KeyBERT


kw_model = KeyBERT()


def gen_keybert_kwords(text: str) -> List[str]:
    res = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 2), stop_words=None)
    return [x[0] for x in res]
