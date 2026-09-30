from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "status",
        F.lower("status").alias("status_lower"),
        F.concat("method", F.lit(" - "), "status").alias("method_status"),
    )
    .orderBy("status_lower")
)
