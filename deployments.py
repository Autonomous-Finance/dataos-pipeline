import os

from prefect import serve
from flows import fetch_files_from_arweave


if __name__ == "__main__":
    fetch_files_from_arweave_deploy = fetch_files_from_arweave.to_deployment(name=os.environ['DEPLOYMENT_NAME'])
    # fast_deploy = fast_flow.to_deployment(name="fast")
    serve(fetch_files_from_arweave_deploy) # add flows as args here