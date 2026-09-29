from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") >= "2023-05-01") | F.col("country").isNotNull())
    .select("id", "device", "started_at", F.length("device").alias("device_length"))
)
