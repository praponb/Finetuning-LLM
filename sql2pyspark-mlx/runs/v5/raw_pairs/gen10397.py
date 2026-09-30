from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "device",
        "customer_id",
        F.upper("device").alias("device_upper"),
        F.month("started_at").alias("started_at_month"),
    )
)
