from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "started_at",
        F.substring("device", 1, 2).alias("device_prefix"),
        F.initcap(F.trim("device")).alias("device_title"),
    )
    .orderBy(F.col("device_prefix").desc())
)
