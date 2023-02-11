from etl_web_to_gcs import parent_flow
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=parent_flow,
    name="q2", 
    version=1, 
)
deployment.apply()