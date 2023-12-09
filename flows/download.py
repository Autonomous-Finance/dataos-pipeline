from prefect import task, flow
import httpx
from datetime import datetime
from flows.util.clickhouse import get_clickhouse_connection
from itertools import cycle
import random
import asyncio


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

clickhouse_client = get_clickhouse_connection()

@task
async def get_file_ids(base_table):
    query = f"""
    WITH q AS (
        SELECT id, created_at_dt, created_at
        FROM dataos_explore.indexable
        ANTI JOIN dataos_explore.file_cache f USING (created_at_dt, id)
        LIMIT 10000
    )
    SELECT DISTINCT id, created_at_dt, created_at
    FROM q
    ORDER BY randCanonical()
    LIMIT 1000;
    """
    result = clickhouse_client.execute(query)
    return [row for row in result]

async def fetch_file(client, gateway_url, file_id, extra_data):
    try:
        # print("trying ", f"{gateway_url}{file_id}")
        response = await client.get(f"{gateway_url}{file_id}", timeout=15, follow_redirects=True)
        if response.status_code == 200:
            return (file_id, extra_data["created_at_dt"], extra_data["created_at"], response.text, datetime.now())
    except Exception as e:
        pass
        #print(f"Error for URL {gateway_url}{file_id}: {e}")
    return (file_id, None, datetime.now())

@task
async def download_files(files):
    print(f"downloading {len(files)}")
    gateway_cycle = cycle(GATEWAYS)
    async with httpx.AsyncClient(verify=False) as client:
        jobs = []
        for file in files:
            jobs.append(
                fetch_file(client, next(gateway_cycle), file[0], {"created_at_dt": file[1], "created_at": file[2]})
            )
        # Use asyncio.gather to run all the fetch operations in parallel
        results = await asyncio.gather(*jobs)

        successful_downloads = []
        failed_downloads = []

        for result in results:
            if result[1]:
                successful_downloads.append(result)
            else:
                failed_downloads.append(result)

        print("download finished")
        # Insert successful results into the database

        # Print statistics
        print(f"Total requests: {len(files)}")
        print(f"Successful downloads: {len(successful_downloads)}")
        print(f"Failed downloads: {len(failed_downloads)}")

        if len(failed_downloads) > len(successful_downloads):
            raise Exception("too many failures")
        return successful_downloads

@task
def insert_to_clickhouse(successful_downloads):
    query = "INSERT INTO dataos_explore.file_cache (id, created_at_dt, created_at, content, retrieved_at) VALUES"
    settings = {} # "async_insert": 1, "async_insert_deduplicate": 0
    clickhouse_client.execute(query, successful_downloads, settings=settings)
    print(f"Saved {len(successful_downloads)} files.")


# Define the Prefect flow
@flow(name="Download and Save Files", log_prints=True)
async def fetch_files_from_arweave(base_table='dataos_relevant_tx_mirror_paragraph'):
    ids = await get_file_ids(base_table)
    successful_files = await download_files(ids)
    insert_to_clickhouse(successful_files)

if __name__ == "__main__":
    # Create and serve the deployment
    fetch_files_from_arweave.serve(name="my-first-deployment")
