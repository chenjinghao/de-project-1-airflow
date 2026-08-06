# Deployment Guide: Airflow Project on Google Cloud Platform (GCP)

This guide provides step-by-step instructions to deploy your Airflow project to Google Cloud Platform using **Cloud Composer 2** (managed Airflow), **Cloud SQL** (PostgreSQL), and **Cloud Storage** (GCS).

## Architecture Overview

*   **Orchestrator:** Google Cloud Composer 2 (Airflow on GKE Autopilot).
*   **Database:** Cloud SQL for PostgreSQL (`stocks_db`).
*   **Object Storage:** Google Cloud Storage (GCS) - replaces MinIO.
*   **CI/CD:** GitHub Actions.

---

## Prerequisites

1.  **GCP Project:** A Google Cloud Project with billing enabled.
2.  **Permissions:** You need `Owner` or `Editor` role on the project to set up initial resources.
3.  **Enabled APIs:** Go to **APIs & Services > Library** and enable:
    *   Cloud Composer API
    *   Cloud SQL Admin API
    *   Cloud Storage API
    *   Compute Engine API
    *   Service Usage API

---

## Step 1: Network & Security Setup

### Create a Service Account
1.  Go to **IAM & Admin > Service Accounts**.
2.  Click **Create Service Account**.
3.  **Name:** `airflow-composer-sa`
4.  **Description:** Service Account for Airflow Composer environment.
5.  **Roles:**
    *   `Composer Worker` (Required for Airflow workers)
    *   `Cloud SQL Client` (To connect to Cloud SQL)
    *   `Storage Object Admin` (To read/write GCS buckets)
    *   `Service Account User` (For Composer agent)
6.  Click **Done**.

### Create Service Account Key (for CI/CD)
1.  Click on the newly created Service Account (`airflow-composer-sa`).
2.  Go to the **Keys** tab.
3.  Click **Add Key > Create new key**.
4.  Select **JSON** and click **Create**.
5.  **Save this file!** You will need it for GitHub Secrets.

---

## Step 2: Cloud SQL Setup (PostgreSQL)

1.  Go to **SQL** in the GCP Console.
2.  Click **Create instance** > **Choose PostgreSQL**.
3.  **Instance ID:** `stocks-db-instance`
4.  **Password:** Generate or choose a strong password (save this!).
5.  **Database version:** PostgreSQL 13 (or higher).
6.  **Region:** Choose the same region as your Composer environment (e.g., `us-central1`).
7.  **Configuration:**
    *   **Zonal availability:** Single zone (for dev/test) or HA (for prod).
    *   **Machine type:** Shared core (e.g., `db-f1-micro`) is cheapest for testing, but `Standard` is recommended for production.
    *   **Connections:** Enable **Public IP** (simplest for initial setup) or **Private IP** (best security). If using Public IP, you must add the IP ranges of your Composer environment to the "Authorized Networks" later, or use the Cloud SQL Proxy (Composer uses Auth Proxy by default with standard connection config).
8.  Click **Create Instance**.

### Create Database and User
1.  Once the instance is ready, click on it.
2.  Go to **Databases** tab > **Create Database**.
    *   **Name:** `stocks_db`
    *   Click **Create**.
3.  Go to **Users** tab > **Add User Account**.
    *   **User name:** `postgres_user` (or your preferred name)
    *   **Password:** (Set a password)
    *   Click **Add**.

---

## Step 3: Cloud Storage Setup

### Create Data Bucket
1.  Go to **Cloud Storage > Buckets**.
2.  Click **Create**.
3.  **Name:** `bronze-<your-project-id>` (Bucket names must be globally unique).
4.  **Location:** Same region as Composer.
5.  Click **Create**.

### Enable S3 Interoperability (Migrating from MinIO)
Since your code uses the `minio` client, we will use GCS's S3 interoperability feature to avoid rewriting all code.

1.  Go to **Cloud Storage > Settings**.
2.  Click on the **Interoperability** tab.
3.  Click **Enable Interoperability Access**.
4.  Scroll to **User access keys**.
5.  Click **Create a key** for the Service Account you created in Step 1 (`airflow-composer-sa`).
    *   *Note: If you don't see the SA, create a key for your user, but for production, it's better to use the SA. However, the MinIO client needs an Access Key and Secret Key.*
6.  **Copy the Access Key and Secret Key.** You will use these in the Airflow Connection.

---

## Step 4: Cloud Composer Setup

1.  Go to **Composer** in the GCP Console.
2.  Click **Create environment** > **Composer 2**.
3.  **Name:** `airflow-stocks-env`
4.  **Location:** `us-central1` (must match Cloud SQL/GCS region for best performance).
5.  **Image version:** `composer-3-airflow-3.1.0-build.7` or later (MUST use Airflow 3+ as the code depends on `airflow.sdk`).
6.  **Service Account:** Select `airflow-composer-sa`.
7.  **Environment resources:** Small (for testing).
8.  **PyPI Packages:**
    *   Expand the **PyPI Packages** section (or add them later via "PyPI Packages" tab).
    *   Add the contents of your `requirements.txt`. **It is critical to add all required packages.** Specifically, ensure you include:
        *   `apache-airflow-providers-slack`
        *   `apache-airflow-providers-http`
        *   `apache-airflow-providers-postgres`
        *   `astro-sdk-python`
        *   `minio`
        *   `astronomer-cosmos`
        *   `dbt-core`
        *   `dbt-postgres`
        *   `pandas`
        *   `pandas_market_calendars`
        *   `pendulum`
        *   `dlt`
9.  **Environment Variables:**
    *   `DBT_TARGET`: `prod` (or `dev`)
10. Click **Create**. (This takes 15-25 minutes).

---

## Step 5: Airflow Connections

Once the Composer environment is running, click **Airflow UI** to open the Airflow web interface.

Go to **Admin > Connections** and add the following:

### 1. Database Connection (`postgres_stock`)
*   **Conn Id:** `postgres_stock`
*   **Conn Type:** `Postgres`
*   **Host:** The Public IP (or Private IP) of your Cloud SQL instance.
*   **Schema:** `stocks_db`
*   **Login:** `postgres_user`
*   **Password:** The user password you created.
*   **Port:** `5432`

### 2. Storage Connection (`minio`)
*   **Conn Id:** `minio`
*   **Conn Type:** `Generic` (or `Amazon S3`)
*   **Extra:**
    ```json
    {"endpoint_url": "https://storage.googleapis.com"}
    ```
*   **Login:** The **Access Key** from Step 3 (Interoperability).
*   **Password:** The **Secret Key** from Step 3.

### 3. API Connection (`stock_api`)
*   **Conn Id:** `stock_api`
*   **Conn Type:** `HTTP`
*   **Host:** `https://www.alphavantage.co/query`
*   **Password:** Your Alpha Vantage API Key.

### 4. Slack Connection (`slack`)
*   **Conn Id:** `slack`
*   **Conn Type:** `Slack API Post`
*   **Password:** Your Slack Bot Token (starts with `xoxb-`).

---

## Step 6: CI/CD Pipeline (GitHub Actions)

We have created a workflow file `.github/workflows/deploy_to_composer.yaml`. To make it work:

1.  In your GitHub Repository, go to **Settings > Secrets and variables > Actions**.
2.  Add the following **Repository Secrets**:
    *   `GCP_PROJECT_ID`: Your GCP Project ID.
    *   `GCP_SA_KEY`: The content of the JSON key file you downloaded in Step 1.
    *   `COMPOSER_BUCKET`: The name of the GCS bucket created by Composer for DAGs (e.g., `us-central1-airflow-stocks-e-bucket-name`). You can find this in the Composer Environment Configuration page under "DAGs folder". *Do not include the `gs://` prefix or `/dags` suffix.* (e.g., just `us-central1-...-bucket`).

3.  Push your code to `main`. The action will trigger and sync your `dags/` and `include/` folders to the Composer bucket.

---

## Step 7: Verification

1.  Wait for the GitHub Action to complete.
2.  Open the **Airflow UI**.
3.  Verify that `most_active_dag` appears and has no import errors.
4.  Trigger the DAG manually and check logs for successful task execution.
