from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "order_id",
        "method",
        "paid_at",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
    )
    .orderBy(F.col("method").desc())
)
