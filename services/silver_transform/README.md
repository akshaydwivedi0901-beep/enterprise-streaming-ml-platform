Silver Transform — Two Implementations Explained
✅ Which one should I use?
File	When to use	Requires
`spark_silver.py`	Large data (>1GB), production Spark cluster, Delta Lake	PySpark, Spark cluster
`transform.py`	Small data, local dev, no Spark cluster available	boto3, pandas, pyarrow
spark_silver.py (PySpark + Delta Lake)
Reads Bronze JSON from S3 using PySpark
Deduplicates on `event_id`
Writes Silver in Delta format (supports ACID, time travel, schema enforcement)
Use this in production — it's what the Airflow DAG calls
transform.py (Pandas + boto3)
Reads Bronze JSON from S3 file-by-file using boto3
Tracks processed files via a checkpoint JSON in S3 (incremental processing)
Adds feature engineering: `device_risk_score`, `country_risk_flag`
Writes Silver as Parquet (simpler, no Delta)
Use this for local testing without a Spark cluster
Decision for this project
The Airflow DAG uses `spark_silver.py` via SparkSubmitOperator.
`transform.py` is kept as a lightweight fallback for local development.