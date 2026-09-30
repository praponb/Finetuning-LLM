from pyspark.sql import functions as F

base = df_transactions.filter(F.col("amount").between(1900, 3500))

summary = (
    base
    .groupBy("txn_type", F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    summary
    .filter(F.col("unique_txn_types") > 7)
    .select("txn_type", "txn_month", "unique_txn_types")
    .orderBy("unique_txn_types")
)
