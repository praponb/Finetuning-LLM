from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2024-02-17").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_clean"),
    )
)

result = (
    base
    .groupBy("customer_id")
    .pivot("country", ["US", "JP", "UK"])
    .agg(F.sum("days_since_started_at"))
)
