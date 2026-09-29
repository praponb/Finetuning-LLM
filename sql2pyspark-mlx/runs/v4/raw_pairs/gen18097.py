from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "REFUNDED") & (F.col("order_id").isin([8, 10, 18]) | (F.col("amount") <= 1370)))
    .select(
        "id",
        "paid_at",
        "status",
        "method",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
        F.round("amount", 0).alias("amount_rounded"),
    )
)
