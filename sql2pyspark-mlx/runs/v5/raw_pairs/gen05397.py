from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(7, 14))
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") > 60)
    .orderBy(F.col("amount_range").desc(), "txn_month")
)
