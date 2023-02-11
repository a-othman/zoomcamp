from prefect.deployments import Deployment  
from prefect_github.repository import GitHubRepository


github_repository_block = GitHubRepository.load("gh-block")
github_repository_block.get_directory()

from etl_web_to_gcs import parent_flow
deployment = Deployment.build_from_flow(
    flow=parent_flow,
    name="q4",

)

deployment.apply()