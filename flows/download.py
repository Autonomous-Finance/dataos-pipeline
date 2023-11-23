from prefect import task, flow
import httpx
from clickhouse_driver import Client
from datetime import datetime
from itertools import cycle
import os
import random
import asyncio
from traceback import print_exc
import ssl


def get_ario_gateways():
    req = httpx.get('https://dev.arns.app/v1/contract/bLAgYxAdX2Ry-nt6aH2ixgvJXbpsEYm28NgJgyqfs-U/gateways')
    gateway_response = req.json()
    gateways = []

    for id, gateway in gateway_response["gateways"].items():
        gateways.append(gateway["settings"]["protocol"] + '://' + gateway["settings"]["fqdn"] + '/')

    return gateways

BASE_GATEWAYS = ["https://arweave.net/", "https://g8way.io/", "https://arweave.dev/", "https://ar-io.dev/"]
ARIO_GATEWAYS = get_ario_gateways()
GATEWAYS = list(set(BASE_GATEWAYS + ARIO_GATEWAYS))
random.shuffle(GATEWAYS)

clickhouse_client = Client(host=os.environ['CLICKHOUSE_HOST'], port=os.environ['CLICKHOUSE_PORT'],
                user=os.environ['CLICKHOUSE_USER'], password=os.environ['CLICKHOUSE_PASSWORD'],
                database=os.environ['CLICKHOUSE_DB'])

@task
async def get_file_ids(base_table):
    query = f"""
    WITH q AS (
        SELECT id
        FROM dataos_explore.dataos_relevant_tx_mirror_paragraph
        LEFT JOIN dataos_explore.files f USING (id)
        WHERE f.content = ''
        LIMIT 10000
    )
    SELECT *
    FROM q
    ORDER BY randCanonical()
    LIMIT 1000
    """
    result = clickhouse_client.execute(query)
    return [row[0] for row in result]

async def fetch_file(client, gateway_url, file_id):
    try:
        # print("trying ", f"{gateway_url}{file_id}")
        response = await client.get(f"{gateway_url}{file_id}", timeout=15, follow_redirects=True)
        if response.status_code == 200:
            return (file_id, response.text, datetime.now())
    except Exception as e:
        pass
        #print(f"Error for URL {gateway_url}{file_id}: {e}")
    return (file_id, None, datetime.now())

@task
async def download_files(ids):
    print(f"downloading {len(ids)}")
    async with httpx.AsyncClient(verify=False) as client:
        gateway_cycle = cycle(GATEWAYS)
        # Use asyncio.gather to run all the fetch operations in parallel
        results = await asyncio.gather(*(fetch_file(client, next(gateway_cycle), file_id) for file_id in ids))

        successful_downloads = []
        failed_downloads = []

        for result in results:
            if result[1]:
                successful_downloads.append(result)
            else:
                failed_downloads.append(result)

        # Insert successful results into the database
        if successful_downloads:
            query = "INSERT INTO dataos_explore.files (id, content, retrieved_at) VALUES"
            clickhouse_client.execute(query, successful_downloads)
            print(f"Saved {len(successful_downloads)} files.")

        # Print statistics
        print(f"Total requests: {len(ids)}")
        print(f"Successful downloads: {len(successful_downloads)}")
        print(f"Failed downloads: {len(failed_downloads)}")

# Define the Prefect flow
@flow(name="Download and Save Files", log_prints=True)
async def fetch_files_from_arweave(base_table='dataos_relevant_tx_mirror_paragraph'):
    ids = await get_file_ids(base_table)
    await download_files(ids)

if __name__ == "__main__":
    # Create and serve the deployment
    fetch_files_from_arweave.serve(name="my-first-deployment")
