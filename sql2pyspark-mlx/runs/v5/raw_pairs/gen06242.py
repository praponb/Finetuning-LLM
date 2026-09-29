from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "method",
        F.concat_ws(",", "method", "status").alias("method_status_key"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
)
