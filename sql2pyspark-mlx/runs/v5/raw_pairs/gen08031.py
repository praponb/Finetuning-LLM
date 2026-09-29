from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("method") == "bank_transfer") & F.col("order_id").isin([19, 35, 38]))
)

payments_stats = (
    base
    .groupBy("method", "status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = payments_stats
