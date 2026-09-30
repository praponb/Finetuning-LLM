from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        F.lower("device").alias("device_lower"),
        F.trim("country").alias("clean_country"),
    )
)
