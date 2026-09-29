from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "order_id")
    .pivot("status", ["SUCCESS", "REFUNDED"])
    .agg(F.avg("amount").alias("total"))
    .select(
        "method",
        "order_id",
        F.col("SUCCESS").alias("success"),
        F.col("REFUNDED").alias("refunded"),
    )
)
