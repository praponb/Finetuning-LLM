from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("order_id").isin([10, 11, 21, 37]) | F.col("amount").between(850, 1490))
    .withColumn("amount_scaled", F.round((F.col("amount") * 1.15), 2))
)

payments_stats = (
    valid_payments
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"))
)

result = payments_stats.orderBy(F.col("paid_day").desc(), F.col("cash_count").desc())
