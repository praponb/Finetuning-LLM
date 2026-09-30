from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        "customer_id",
        "started_at",
        F.trim("country").alias("clean_country"),
        F.upper("country").alias("country_upper"),
    )
    .orderBy("device", "started_at")
)
