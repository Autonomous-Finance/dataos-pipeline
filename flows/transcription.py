import os
import uuid

import torch
import json
from faster_whisper import WhisperModel
import requests
from flows.util.clickhouse import get_ch_client
from datetime import datetime
from pathlib import Path
from prefect import task, flow, serve
#from flows.metadata_gpu import generate_gpu_metadata_flow

def transcribe(video_url: str):
    storage_folder = 'videos_etl/data'
    Path(storage_folder).mkdir(parents=True, exist_ok=True)
    response = requests.get(video_url)

    if response.status_code == 200:
        try:
            page_content = response.content
            page_str = page_content.decode()
            # This part is specific for odysee
            x = page_str[page_str.find('"contentUrl"'):]
            video_url = x.split('"')[3]
            print('video_url:', video_url)

            response = requests.get(video_url, stream=True)

            local_file_path = f'{storage_folder}/local_video.mp4'

            if response.status_code == 200:
                with open(local_file_path, 'wb') as local_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        local_file.write(chunk)
                print('file saved locally')
            else:
                print(f"status code: {response.status_code}")
        except:
            print('failed to extract video')

    def check_gpu():
        check = torch.cuda.is_available()
        if check:
            print('cuda.is_available')
        else:
            print('cuda is not available')
        return check

    model_size = "large-v3"

    if check_gpu():
        # Run on GPU with FP16
        model = WhisperModel(model_size, device="cuda", compute_type="float16")

        # or run on GPU with INT8
        # model = WhisperModel(model_size, device="cuda", compute_type="int8_float16")
    else:
        # or run on CPU with INT8
        model = WhisperModel(model_size, device="cpu", compute_type="int8")

    segments, info = model.transcribe(local_file_path, beam_size=5)

    os.remove(local_file_path)

    res_list = []
    file_id = str(uuid.uuid4())

    k = 0
    for segment in segments:
        # print("[%.2fs -> %.2fs] %s" % (segment.start, segment.end, segment.text))
        res_list.append({"file_id": file_id, "file_url": video_url, "text": segment.text,
                         "segment_start": segment.start, "segment_end": segment.end, "segment_id": k})
        k += 1

    return res_list


@task
def load_and_transcribe(urls: list[str]):
    ch_client = get_ch_client()

    results = []
    for url in urls:
        transcription_result = transcribe(url)
        transcription_fulltext = ' '.join(map(lambda r: r['text'], transcription_result))
        results.append([url, transcription_result[0]['file_url'], datetime.now(),
                        json.dumps(transcription_result), transcription_fulltext])


    ch_client.insert(
        'dataos_explore.video_transcriptions',
        results,
        column_names=['source_url', 'download_url', 'created_at', 'transcription_json', 'fulltext']
    )


@flow(name="Generate audio transcription", log_prints=True)
def transcription_flow(urls: list[str]):
    load_and_transcribe(urls)
    #generate_gpu_metadata_flow(app='odysee')


if __name__ == "__main__":
    transcription_flow.serve(name=os.environ['DEPLOYMENT_NAME'])
