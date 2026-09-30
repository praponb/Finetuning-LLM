from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet"]) & F.col("customer_id").isin([18, 20, 29, 32]) & F.col("country").isin(["TH", "DE"]))
    .select(
        "id",
        "device",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.lower("device").alias("device_lower"),
    )
)
