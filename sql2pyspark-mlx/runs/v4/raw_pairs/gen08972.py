from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-11-29 23:00:00").cast("timestamp"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("Unknown")))
    .withColumn("started_at_hour", F.hour("started_at"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("N/A")))
)

result = (
    valid
    .groupBy("browser")
    .pivot("device", ["mobile", "desktop", "tablet"])
    .agg(F.max("started_at_hour").alias("total"), F.count(F.lit(1)).alias("n"))
)
