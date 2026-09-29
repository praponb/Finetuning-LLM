from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & F.col("order_id").isNotNull())
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") >= 510)
)
