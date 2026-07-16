# URL Integrity Engine - Bemol Automation

This repository contains the Databricks automation scripts used for normalizing and correcting product URLs (slugs) in VTEX to improve SEO consistency for Bemol.

## Architecture

The automation is split into sequential Databricks Notebooks (Python) intended to run inside a Databricks Job/Workflow.

### 1. Main Orchestrator (`app.py`)
This script executes the core business logic of the automation:
- **Fetch Queue**: Reads the next SKUs requiring normalization from the source Delta table.
- **Idempotency**: Prevents reprocessing of successfully corrected or intentionally skipped products.
- **Validation & Transformation**: Connects to the VTEX API to fetch current active products and normalizes the slug (e.g., lowercase conversion, special character removal).
- **Collision Handling**: Handles slug conflicts dynamically by appending deterministic suffixes (e.g., `-skuId`) and ensures no duplicate URLs are generated.
- **Redirection (301)**: Creates SEO-friendly 301 redirects within the VTEX environment to point old URLs to the newly normalized endpoints, guaranteeing no 404 errors.
- **Delta Logging**: Inserts execution results, latency, and comprehensive statuses into the destination Databricks Delta Log table using an explicit schema.

### 2. Email Reporter (`envio_email.py`)
Runs sequentially after the Orchestrator, reporting the status of the batch:
- Retrieves the correct `execution_id` from the Databricks Job Task Values.
- Connects to the Delta Log table (bypassing eager cache using `REFRESH TABLE` and `COALESCE` optimized max-timestamp aggregation).
- Generates a styled HTML Email matching Bemol's corporate reporting standards.
- Details the number of successful updates, errors, skips (already processed/inactive products), and alerts the SEO team for critical scenarios requiring manual intervention (e.g., PUT succeeded but Redirect failed).

## Configuration

Secrets and credentials must not be stored in the repository. The project strictly relies on Databricks Secrets and local `.env` setups:
- `SMTP_PASSWORD`: Retrieved securely via `dbutils.secrets.get(scope="smtp", key="password")`.
- `VTEX_APP_KEY` / `VTEX_APP_TOKEN`: Required for VTEX Catalog API communication.

## Execution

For local testing, notebooks can be run iteratively. The fallback mechanics ensure the latest logs are retrieved from the Delta tables even if Job Contexts (`TaskValues`) are not available. In production, run this under a scheduled Databricks Workflow batch.
