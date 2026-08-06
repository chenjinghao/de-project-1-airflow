# Deploying Airflow and dbt to Google Cloud Platform (GCP)

This guide outlines how to deploy this Airflow project (which includes dbt transformations) to GCP.

## Architecture Overview

*   **Orchestration:** Airflow (deployed via Astronomer or Cloud Composer).
*   **Database (Warehouse):** Google Cloud SQL for PostgreSQL.
    *   This project is configured to use Postgres for both the Airflow metadata database (managed by Astro/Composer) and the Data Warehouse (managed by you).
*   **Object Storage:** Google Cloud Storage (GCS).
    *   Replaces MinIO for storing raw JSON data extracted from APIs.
*   **Transformation:** dbt (running within Airflow tasks via Cosmos).

## Prerequisites

1.  **GCP Project:** A Google Cloud Platform project with billing enabled.
2.  **Tools:**
    *   `gcloud` CLI installed and authenticated.
    *   `astro` CLI (if deploying to Astronomer).

## Step 1: Infrastructure Setup

### 1. Google Cloud SQL (Postgres)

This project requires a Postgres database to store stock data.

1.  **Create Instance:**
    *   Go to **Cloud SQL** in GCP Console.
    *   Create a **PostgreSQL** instance.
    *   Choose an appropriate region and machine type (e.g., `db-custom-1-3840` for dev).
    *   **Network:** Ensure "Public IP" is enabled (with authorized networks restricted to your Airflow IP) OR use "Private IP" if your Airflow environment is in the same VPC.

2.  **Create Database & User:**
    *   Create a database named `stocks_db`.
    *   Create a user (e.g., `stock_user`) with a password.

### 2. Google Cloud Storage (GCS)

This project uses object storage for intermediate files. We will use GCS with S3 Interoperability.

1.  **Create Bucket:**
    *   Go to **Cloud Storage** -> **Buckets**.
    *   Create a bucket named `bronze` (or a unique name like `my-project-bronze`).
2.  **Enable Interoperability:**
    *   Go to **Cloud Storage** -> **Settings** -> **Interoperability**.
    *   Enable Interoperability API.
    *   Create a **Key for Service Accounts** (or User Account).
    *   Note down the **Access Key** and **Secret Key**.

## Step 2: Airflow Configuration

You need to configure Airflow Connections to point to your GCP resources.

### 1. Postgres Connection (`postgres_stock`)

*   **Conn Id:** `postgres_stock`
*   **Conn Type:** `Postgres`
*   **Host:** Your Cloud SQL Instance IP address.
*   **Schema:** `stocks_db`
*   **Login:** `stock_user`
*   **Password:** `<your-password>`
*   **Port:** `5432`

### 2. MinIO/GCS Connection (`minio`)

We reuse the `minio` connection but point it to GCS.

*   **Conn Id:** `minio`
*   **Conn Type:** `Amazon Web Services` (or `Generic`)
*   **Login:** `<Your GCS Access Key>`
*   **Password:** `<Your GCS Secret Key>`
*   **Extra:**
    ```json
    {
      "endpoint_url": "https://storage.googleapis.com"
    }
    ```
    *Note: The project code has been updated to handle `storage.googleapis.com` correctly.*

## Step 3: Deployment

### Option A: Deploy to Astronomer (Recommended)

1.  **Initialize Astro Project:** (Already done)
2.  **Authenticate:**
    ```bash
    astro login
    ```
3.  **Create Deployment:**
    ```bash
    astro deployment create my-stock-deployment --executor CeleryExecutor
    ```
4.  **Deploy Code:**
    ```bash
    astro deploy
    ```
5.  **Set Connections:**
    *   Use the Airflow UI (Admin -> Connections) on your Astro deployment to set the `postgres_stock` and `minio` connections as described above.

### Option B: Deploy to Cloud Composer

1.  **Create Environment:** Create a Cloud Composer environment.
2.  **Upload DAGs:** Upload the content of `dags/` to the DAGs bucket in Composer.
3.  **Upload Plugins/Include:** Upload `include/` to the `plugins/` folder (or handle python path accordingly).
    *   *Note:* You might need to adjust `PYTHONPATH` or restructure the project for Composer.
4.  **PyPI Packages:** Add `dbt-core`, `dbt-postgres`, `astronomer-cosmos`, `minio` to the PyPI packages in Composer configuration.

## Step 4: Environment Variables (Optional)

You can switch the dbt target environment by setting:
*   `DBT_TARGET`: `prod` (default is `dev`)

This requires updating `include/dbt/profiles.yml` or relying on dynamic profile mapping.
