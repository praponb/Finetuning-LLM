from pyspark.sql import functions as F

base = df_transactions.filter(F.col("account_id").isin([5, 9, 38]) | F.col("is_flagged"))

txn_month_summary = (
    base
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.count("*").alias("num_transactions"),
        F.count("*").alias("cnt"),
    )
)

result = (
    txn_month_summary
    .filter(F.col("cnt") > 6)
    .select("txn_month", "num_transactions", "cnt")
)
