from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") < "2023-09-01") | (F.col("device") == "mobile"))
    .select("id", "customer_id", "browser", F.trim("device").alias("clean_device"))
    .orderBy("customer_id")
)
