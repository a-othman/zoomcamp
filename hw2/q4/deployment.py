from prefect.deployments import Deployment  
from prefect_github.repository import GitHubRepository

github_repository_block = GitHubRepository.load("gh-block")


deployment = Deployment.build_from_flow(
    flow=my_flow,
    name="q4",
    storage=github_repository_block,

)

deployment.apply()