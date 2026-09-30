from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-07-24 06:00:00"))) & (F.col("method") == "promptpay"))
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.when(F.col("method") == "paypal", 1).otherwise(0).alias("is_paypal"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

result = (
    filtered
    .groupBy("order_id", "status")
    .pivot("method", ["paypal", "promptpay"])
    .agg(F.sum("is_paypal").alias("amount"))
)
