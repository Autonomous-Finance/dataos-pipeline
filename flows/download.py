from prefect import task, flow
import requests
from clickhouse_driver import Client
from datetime import datetime
from itertools import cycle
import os
import random

# todo incorporate
# https://dev.arns.app/v1/contract/bLAgYxAdX2Ry-nt6aH2ixgvJXbpsEYm28NgJgyqfs-U/gateways
# https://gateways.ar-io.dev/
GATEWAYS = ["https://arweave.net/", "https://g8way.io/", "https://arweave.dev/",
            "https://ar-io.dev/", "https://xyznodes.site/", "https://learnandhunt.me/",
            "https://saktinaga.live/", "https://hellocryptoworld.store/", "https://coolqas.tech/",
            "https://thecoldblooded.net/", "https://bootstrap.lol/", "https://boramir.store/", "https://maclaurino.xyz/"]



client = Client(host=os.environ['CLICKHOUSE_HOST'], port=os.environ['CLICKHOUSE_PORT'],
                user=os.environ['CLICKHOUSE_USER'], password=os.environ['CLICKHOUSE_PASSWORD'],
                database=os.environ['CLICKHOUSE_DB'])

@task
def get_file_ids(base_table):
    query = f"""
    WITH q AS (
        SELECT id
        FROM dataos_explore.dataos_relevant_tx_mirror_paragraph
        SAMPLE 10000
    )
    SELECT *
    FROM q
    LEFT JOIN dataos_explore.files f
    USING (id) WHERE f.content = ''
    LIMIT 100
    """
    result = client.execute(query)
    return [row[0] for row in result]

@task
def download_files(ids):
    for file_id in ids:
        shuffled_gateways = random.sample(GATEWAYS, len(GATEWAYS))
        gateway_cycle = cycle(shuffled_gateways)

        for _ in range(len(GATEWAYS)):
            base_url = next(gateway_cycle)
            try:
                print("trying ", f"{base_url}{file_id}")
                response = requests.get(f"{base_url}{file_id}", timeout=1)
                if response.status_code == 200:
                    retrieved_at = datetime.now()
                    query = "INSERT INTO dataos_explore.files (id, content, retrieved_at) VALUES"
                    values = [(file_id, response.text, retrieved_at)]
                    client.execute(query, values)
                    print("saved")
                    break
            except requests.RequestException:
                print("error")
                continue



# Define the Prefect flow
@flow(name="Download and Save Files", log_prints=True)
def fetch_files_from_arweave(base_table='dataos_relevant_tx_mirror_paragraph'):
    ids = get_file_ids(base_table)
    download_files(ids)

if __name__ == "__main__":
    # Create and serve the deployment
    fetch_files_from_arweave.serve(name="my-first-deployment")
