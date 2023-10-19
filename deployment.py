# -*- coding: utf-8 -*-
from main import pipeline
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=pipeline,
    name="Financial Historical Data Ingestion Pipeline",
)

if __name__ == "__main__":
    deployment.apply()
