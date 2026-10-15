# Offline Data Directory

This directory contains offline sample datasets, schemas, and test fixtures:

- **`sample_transactions.json`**: Formatted JSON array containing 50 representative financial transactions, including normal spending and labeled fraud patterns (high value, rapid transfer bursts, card declines, spending spikes).
- **`sample_transactions.csv`**: Delimited CSV representation of the sample transactions for ad-hoc SQL, pandas, or spreadsheet inspection.

> **Note on Data Lake Storage:**
> - The live real-time streaming Parquet data lake is stored in [`lake/`](../lake/), divided into `lake/bronze/` (raw payloads with Kafka metadata) and `lake/silver/` (cleaned, typed events).
