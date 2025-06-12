# Civic Pulse – Rochester

These steps outline how to run the Streamlit app and the ingestion scripts locally.

## Setup

Create and activate a Python 3.11 virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Install the required packages:

```bash
pip install -r requirements.txt
```

## Streamlit app

Launch the dashboard with:

```bash
streamlit run webapp/pulse_app.py
```

The app expects AWS credentials, `AWS_REGION` and a `MAPBOX_TOKEN` in your environment for data access and map rendering.

## Ingestion scripts

Scripts under `data_ingest/` fetch data and write it to S3. They may be invoked manually by supplying the necessary environment variables. Example:

```bash
AWS_REGION=us-east-1 \
BUCKET=my-bucket \
SOCRATA_APP_TOKEN=<token> \
TARGET_DATE=2024-06-01 \
python data_ingest/fetch_buf_311.py
```

Check each script's docstring for the complete list of variables.
