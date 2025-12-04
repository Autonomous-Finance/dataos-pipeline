from prefect import task, flow
import httpx
from datetime import datetime
from itertools import cycle
import random
import asyncio
from collections import namedtuple


#https://gateways.ar-io.dev/#/
def get_ario_gateways():
    req = httpx.get('https://dev.arns.app/v1/contract/bLAgYxAdX2Ry-nt6aH2ixgvJXbpsEYm28NgJgyqfs-U/gateways')
    gateway_response = req.json()
    gateways = []

    for id, gateway in gateway_response["gateways"].items():
        gateway_url = gateway["settings"]["protocol"] + '://' + gateway["settings"]["fqdn"] + '/'
        # try:
        #     httpx.get(gateway_url + 'ar-io/healthcheck', timeout=httpx.Timeout(1.5), trust_env=True).json()
        # except Exception as e:
        #     print(gateway_url, 'down')
        #     print(e)
        # else:
        #     gateways.append(gateway_url)
        #     print(gateway_url, 'up')
        gateways.append(gateway_url)

    return gateways


BASE_GATEWAYS = ["https://arweave.net/", "https://g8way.io/", "https://arweave.dev/", "https://ar-io.dev/"]
ARIO_GATEWAYS = get_ario_gateways()
GATEWAYS = list(set(BASE_GATEWAYS + ARIO_GATEWAYS))
random.shuffle(GATEWAYS)
gateway_cycle = cycle(GATEWAYS)


DownloadResult = namedtuple('DownloadResult', ['file_id', 'created_date', 'created_at', 'response_str', 'retrieved_at'])

async def fetch_file(client, gateway_url, file_id, extra_data) -> DownloadResult:
    try:
        # print("trying ", f"{gateway_url}{file_id}")
        response = await client.get(f"{gateway_url}{file_id}", timeout=httpx.Timeout(15), follow_redirects=True)
        if response.status_code == 200:
            return DownloadResult(file_id, extra_data["created_at_dt"], extra_data["created_at"], response.text, datetime.now())
    except Exception as e:
        pass
    return DownloadResult(file_id, None, None, None, datetime.now())


@task
async def download_files(files) -> list[DownloadResult]:
    print(f"downloading {len(files)}")
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
