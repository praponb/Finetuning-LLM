from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 2600)
    .select("txn_month", "min_amount")
    .orderBy(F.col("min_amount").desc(), "txn_month")
    .limit(20)
)
