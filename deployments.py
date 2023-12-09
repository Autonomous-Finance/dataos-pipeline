import os

from prefect import serve
from flows import fetch_files_from_arweave, generate_spacy_entities, generate_flair_entities

if __name__ == "__main__":
    fetch_files_from_arweave_deploy = fetch_files_from_arweave.to_deployment(name=os.environ['DEPLOYMENT_NAME'])
    spacy_entities_generation_deploy = generate_spacy_entities.to_deployment(name=os.environ['DEPLOYMENT_NAME'])
    generate_flair_entities_deploy = generate_flair_entities.to_deployment(name=os.environ['DEPLOYMENT_NAME'])
    # fast_deploy = fast_flow.to_deployment(name="fast")
    serve(fetch_files_from_arweave_deploy, spacy_entities_generation_deploy, generate_flair_entities_deploy) # add flows as args here