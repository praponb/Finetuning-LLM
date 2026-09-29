from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-06-15")
    .select(
        "id",
        "device",
        "customer_id",
        "browser",
        F.concat_ws("/", "country", "device").alias("country_device_key"),
    )
    .orderBy(F.col("id").desc())
)
