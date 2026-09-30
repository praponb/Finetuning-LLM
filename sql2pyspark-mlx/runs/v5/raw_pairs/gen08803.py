from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["TH", "JP"]) & F.col("customer_id").isNotNull() & F.col("customer_id").isin([4, 27]))
    .select(
        "started_at",
        F.length("country").alias("country_length"),
        F.when(F.col("device").isin(["desktop", "tablet"]), "Group A").otherwise("Other").alias("device_group"),
    )
)
