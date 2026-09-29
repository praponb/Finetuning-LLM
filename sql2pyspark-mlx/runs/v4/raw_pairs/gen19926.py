from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "promptpay", "bank_transfer"]) | F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.length("method").alias("method_length"),
        F.upper("method").alias("method_upper"),
    )
)
