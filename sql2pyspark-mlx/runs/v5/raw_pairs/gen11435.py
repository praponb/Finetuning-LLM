from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "REFUNDED") | (F.col("status") == "FAILED"))
    .groupBy("order_id", F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") < 3700)
)
