from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([17, 22, 27, 32]) | (F.col("amount") < 1030))
    .groupBy("method")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
    )
)
