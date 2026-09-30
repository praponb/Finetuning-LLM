from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 560) & (F.col("order_id").isin([3, 5, 31]) | (F.col("status") != "SUCCESS")))
    .groupBy("method")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
)
