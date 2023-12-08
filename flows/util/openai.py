import os
from openai import OpenAI


openai_client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])


MAX_TEXT_SIZE = 15000


def chat_completion(prompt: str):
    chat_completion = openai_client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model="gpt-3.5-turbo",
    )
    return chat_completion.choices[0].message.content


def generate_text_summary(text: str):
    if len(text) > MAX_TEXT_SIZE:
        text = text[:MAX_TEXT_SIZE]
    prompt = f"""Generate me a short summary of text in brackets.

     Text: <{text}>
     """
    return chat_completion(prompt)


def generate_text_title(text: str):
    if len(text) > MAX_TEXT_SIZE:
        text = text[:MAX_TEXT_SIZE]
    prompt = f"""Generate me a title for the text in brackets.

     Text: <{text}>
     """
    return chat_completion(prompt)


def generate_text_style(text: str):
    if len(text) > MAX_TEXT_SIZE:
        text = text[:MAX_TEXT_SIZE]
    prompt = f"""In wihch style written text below?

     Text: {text}
     """
    return chat_completion(prompt)


def get_embeddings(texts: list[str], model="text-embedding-ada-002"):
    response = openai_client.embeddings.create(
      input=texts,
      model=model
    )

    return [x.embedding for x in response.data]
