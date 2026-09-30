from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.initcap(F.trim("device")).alias("device_title"),
    )
)
