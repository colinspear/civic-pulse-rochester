# Civic Pulse Rochester

Please review the README in the root of this repository for an overview of what this project does.

## Data extraction

* Each night, data extractors are run for Open Data via Github actions. These trigger the `fetch_buf` scripts in the `data_ingest` directory.
* After extraction, data is loaded into an S3 bucket.
* In addition to nightly extracts, there is a backfill workflow that can be run manually to backfill data for any of the individual data streams

## Modeling

* `notebooks/pulse_score.ipynb` is a notebook that fetches data from the S3 bucket via AWS Athena, cleans and processes, and constructs the Civic Pulse Index along with an XGBoost model from which SHAP scores are constructed. Index measures and SHAP scores are loaded in the S3.

## Webapp

* Index and SHAP socres are fed to the webapp along with Erie County Census tract information
* These are combined to create a Streamlit app to display the pulse score

## Future plans

* Build an LLM based score summarizer that takes the Index measures and SHAP scores and creates a short, plain language summary of the results.
