from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-23").cast("date")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.trim("device").alias("clean_device"),
        F.length("browser").alias("browser_length"),
    )
)

result = (
    cleaned
    .groupBy("country")
    .pivot("device", ["desktop", "tablet"])
    .agg(F.max("browser_length").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
