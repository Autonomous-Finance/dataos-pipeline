from prefect import task, flow
import httpx
from datetime import datetime
from flows.util.clickhouse import get_clickhouse_connection
from itertools import cycle
import random
import asyncio
from util.arweave import download_files

@task
async def get_file_ids(base_table):
    clickhouse_client = get_clickhouse_connection()
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

@task
def insert_to_clickhouse(successful_downloads):
    query = "INSERT INTO dataos_explore.file_cache (id, created_at_dt, created_at, content, retrieved_at) VALUES"
    settings = {} # "async_insert": 1, "async_insert_deduplicate": 0
    clickhouse_client = get_clickhouse_connection()
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
