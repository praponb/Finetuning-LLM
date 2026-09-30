from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-09-29 01:00:00"))) & F.col("customer_id").isNotNull())
    .withColumn("started_at_weekofyear", F.weekofyear("started_at"))
    .withColumn("browser_prefix", F.substring("browser", 1, 2))
)

result = (
    filtered
    .select("id")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isin([13, 18, 23, 24]))
        .select("id")
    )
)
