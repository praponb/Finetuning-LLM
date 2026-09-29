from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "txn_type")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") > 39)
    .orderBy(F.col("txn_type").desc())
)
