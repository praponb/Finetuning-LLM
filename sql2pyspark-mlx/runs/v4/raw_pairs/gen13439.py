from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

result = (
    base
    .groupBy("browser")
    .pivot("device", ["mobile", "desktop", "tablet"])
    .agg(F.max("days_since_started_at").alias("value_sum"))
)
