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


from flows import check_text_quality
from prefect.deployments import Deployment
from prefect.filesystems import LocalFileSystem

deployment_ctq_gpu = Deployment.build_from_flow(
    flow=check_text_quality,
    name='gpu',
    version=2,
    work_queue_name="check_text_quality",
    work_pool_name="vast-agents",
)
deployment_ctq_gpu.apply()

deployment_ctq_cpu = Deployment.build_from_flow(
    flow=check_text_quality,
    name='cpu',
    version=2,
    work_queue_name="check_text_quality",
    work_pool_name="cpu-agents",
)
deployment_ctq_cpu.apply()