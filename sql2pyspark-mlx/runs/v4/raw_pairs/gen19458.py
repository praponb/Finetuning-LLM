from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "method",
        "paid_at",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
    )
    .orderBy("paid_at", "status_method_key")
)
