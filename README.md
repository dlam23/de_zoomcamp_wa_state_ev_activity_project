# Washington State Electric Vehicle Registration Data Pipeline

The goal of this project is to design and implement an end-to-end data pipeline that ingests Washington State Electric Vehicle (EV) registration activity data from an API into a data warehouse. The Washington State Department of Licensing provides an API that allows access to registration activity of electric vehicles in the state. This data includes information such as vehicle make, model, year, registration date, and owner zip code.

This pipeline would provide stakeholders with access to timely and reliable data on electric vehicle registration activity in Washington State, enabling them to make informed decisions related to EV policy, infrastructure planning, and environmental impact analysis.

This project also fulfills the final requirement to complete the DataTalks.Club Data Engineering Zoomcamp course for 2024.

## Architecture
![Diagram](https://github.com/dlam23/de_zoomcamp_wa_state_ev_activity_project/blob/main/images/Dataflow%20Diagram.png)

### Technologies Used

- Infrastructure as code (IaC): Terraform
- Workflow orchestration: Mage
- Containerization: Docker
- Data Lake: Google Cloud Storage (GCS)
- Data Warehouse: BigQuery
- Transformations: Spark
- Visualization: Looker Studio

## Instructions

### Prerequisites

- Google Cloud CLI installed
- Terraform installed
- Artifact Registry repo to store Docker images
- Service account with following roles:
  - Artifact Registry Reader
  - Artifact Registry Writer
  - BigQuery Admin
  - Cloud Run Developer
  - Cloud SQL Admin
  - Service Account Token Creator
  - Storage Admin
  - Storage Object Admin
  - Storage Object Creator
  - Viewer

### Recreating the Pipeline

1. **Build the Docker Image**:

```sh
cd mage
docker build --tag mageprod:latest .
```

2. **Push Docker image to your Artifact Registry repo**:

```sh
gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://<region>-docker.pkg.dev/<project_id>/my-docker-images
docker tag mageprod:latest <region>-docker.pkg.dev/<project_id>/my-docker-images/mageprod:latest
docker push <region>-docker.pkg.dev/<project_id>/my-docker-images/mageprod:latest
```

3. **After replacing the project_id, region, and docker_image variables in the variables.tf file with your details, build the required cloud resources using Terraform**:

```sh
cd terraform
terraform init
terraform plan
terraform apply
```

4. **Copy Spark script to created storage bucket**:

```sh
gsutil -m cp spark_bigquery.py gs://staging-bucket-<project_id>/code/
```

At this point, the pipeline is active and set to trigger daily at 12:00 UTC.
Replace `<project_id>` and `<region>` with your actual Google Cloud project ID and region, respectively.

### Visualization
The visualization can be found here: https://lookerstudio.google.com/reporting/a5f1a0f1-b51f-48a2-8e1c-18f667982ac7/page/4NxvD

![Diagram](https://github.com/dlam23/de_zoomcamp_wa_state_ev_activity_project/blob/main/images/Dashboard.png)
