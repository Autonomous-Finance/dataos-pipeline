# import os
#
# from prefect import serve
# from flows import fetch_files_from_arweave, check_text_quality
#
# if __name__ == "__main__":
#     fetch_files_from_arweave_deploy = fetch_files_from_arweave.to_deployment(name=os.environ['DEPLOYMENT_NAME'])
#     check_text_quality_deploy = check_text_quality.to_deployment(name=os.environ['DEPLOYMENT_NAME'])
#     # fast_deploy = fast_flow.to_deployment(name="fast")
#     serve(fetch_files_from_arweave_deploy, check_text_quality) # add flows as args here


from flows.metadata_gpu import generate_gpu_metadata_flow
from flows.metadata_cpu import generate_gpt_metadata_flow

from flows.blocks import get_arweave_blocks
from flows.transcription import transcription_flow
from prefect.deployments import Deployment
from prefect.filesystems import LocalFileSystem


deployment_metadata_cpu = Deployment.build_from_flow(
    flow=generate_gpt_metadata_flow,
    name='cpu',
    version=2,
    work_pool_name="cpu-agents",
)
deployment_metadata_cpu.apply()

deployment_metadata_gpu = Deployment.build_from_flow(
    flow=generate_gpu_metadata_flow,
    name='gpu',
    version=2,
    work_pool_name="gpu-agents",
)
deployment_metadata_gpu.apply()


deployment_transcription = Deployment.build_from_flow(
    flow=transcription_flow,
    name='gpu',
    version=2,
    work_pool_name="gpu-agents",
)
deployment_transcription.apply()


deployment_blocks_cpu = Deployment.build_from_flow(
    flow=get_arweave_blocks,
    name='cpu',
    version=2,
    work_pool_name="cpu-agents",
)
deployment_blocks_cpu.apply()
