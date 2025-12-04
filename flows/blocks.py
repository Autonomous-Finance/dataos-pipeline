from prefect import task, flow
import requests
from datetime import datetime
import json
from flows.util.clickhouse import get_ch_client
import os
import random
import flows.util.arweave as arweave
from flows.util.arweave import BASE_GATEWAYS

def transform_block_data(input: arweave.DownloadResult):
    # Step 1: Fetch block data
    try:
        block_data = json.loads(input.response_str)
    except json.JSONDecodeError:
        return None

    timestamp_unix = block_data.get('timestamp')
    block_timestamp = datetime.utcfromtimestamp(int(timestamp_unix))

    height = block_data.get('height')
    inserted_at = datetime.utcnow()

    return [height, block_timestamp, inserted_at, input.response_str, input.response_str]



def get_heights_to_fetch(max_height) -> list[int]:
    ch_client = get_ch_client()
    result = ch_client.query(f'''
    SELECT number AS height
    FROM numbers({max_height}) AS n
    ANTI JOIN default.blocks b ON toInt32(n.number) = b.height
    ORDER BY height
    LIMIT 100000
    ''')
    return list(map(lambda r: r[0], result.result_rows))

def get_current_height():
    gw = random.choice(arweave.BASE_GATEWAYS)
    resp = requests.get(f'{gw}/block/current')
    block = resp.json()
    return block.get('height')

def chunked_iter(lst, chunk_size=100):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

@flow(name="Get Arweave Block Meta", log_prints=True)
async def get_arweave_blocks():
    runs = 200
    current_height = get_current_height()
    blocks_to_fetch = get_heights_to_fetch(current_height)
    jobs = list(map(lambda height: [f'block/height/{height}', True, True], blocks_to_fetch))

    ch_client = get_ch_client()
    for chunk in chunked_iter(jobs, 200):
        print(chunk)
        r = await arweave.download_files(chunk)
        r_transform = list(filter(lambda r: r is not None, map(transform_block_data, r)))
        ch_client.insert('default.blocks', r_transform)
        runs -= 1
        if runs == 0:
            break



if __name__ == "__main__":
    get_arweave_blocks.serve(name=os.environ['DEPLOYMENT_NAME'])
