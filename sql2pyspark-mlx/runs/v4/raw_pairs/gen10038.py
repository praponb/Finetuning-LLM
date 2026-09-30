from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "latency_ms", "status_code", F.explode("context").alias("k", "value"))
)

result = exploded.select("id", "status_code", "k")
