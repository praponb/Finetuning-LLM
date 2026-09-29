from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .withColumn("country_lower", F.lower("country"))
    .withColumn("is_firefox", F.when(F.col("browser") == "Firefox", 1).otherwise(0))
    .withColumn("resolved_country", F.coalesce("country", F.lit("NONE")))
)

result = (
    cleaned
    .groupBy("browser", "customer_id")
    .pivot("country", ["US", "SG"])
    .agg(F.avg("is_firefox").alias("total"))
)
