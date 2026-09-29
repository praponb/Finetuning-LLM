from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 6) & F.col("order_id").isNull())
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"), "method")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
