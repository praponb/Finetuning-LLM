from pyspark.sql import functions as F

result = (
    df_app_logs
    .select("id", "host", "log_ts", "status_code", F.explode("context").alias("k", "v"))
)
