from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-09-15 20:00:00").cast("timestamp"))
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

result = (
    filtered
    .select("customer_id", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "country")
    )
)
