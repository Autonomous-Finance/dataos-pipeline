from transformers import pipeline


bart_classifier = pipeline(
    "zero-shot-classification",
    model="facebook/bart-large-mnli"
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


def gen_bart_categories(text: str) -> dict:
    res = bart_classifier(text, candidate_labels, multi_label=True)
    return dict(zip(res['labels'], res['scores']))


def flatten_bart_categories(data: dict, treshold=0.7) -> list[str]:
    res = []
    for k, v in data.items():
        if v > treshold:
            res.append(k)
        return res
