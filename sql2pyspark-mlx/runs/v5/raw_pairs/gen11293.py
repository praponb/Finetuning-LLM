from pyspark.sql import functions as F

result = (
    df_app_logs
    .select("id", "log_ts", "level", F.explode("context").alias("key", "attr_value"))
)
