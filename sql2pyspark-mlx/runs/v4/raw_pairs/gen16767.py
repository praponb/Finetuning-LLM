from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() & F.col("country").isin(["SG", "DE", "US"]) & F.col("customer_id").isin([17, 40]))
    .select(
        "id",
        "started_at",
        F.coalesce("browser", F.lit("Unknown")).alias("browser_filled"),
    )
)
