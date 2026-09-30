from pyspark.sql import functions as F

base = df_transactions.filter(F.col("amount") <= 2500)

transactions_stats = (
    base
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = transactions_stats.select("txn_month", "stddev_amount", "total_amount")
