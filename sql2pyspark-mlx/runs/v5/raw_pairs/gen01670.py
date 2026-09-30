from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "txn_type", F.round("amount", 2).alias("amount_rounded"))
    .orderBy(F.col("amount_rounded").desc(), "id")
    .limit(10)
)
