from etl_gcs_to_bq import parent_flow
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=parent_flow,
    name="q3", 
    version=1, 
)
deployment.apply()