from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        "customer_id",
        "started_at",
        F.length("country").alias("country_length"),
    )
)
