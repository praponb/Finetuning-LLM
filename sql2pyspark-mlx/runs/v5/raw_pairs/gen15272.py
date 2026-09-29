from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() & (F.col("started_at") >= "2023-04-01") & F.col("customer_id").isNotNull())
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("clean_device", F.trim("device"))
)
