from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "txn_time", F.substring("merchant", 1, 3).alias("merchant_prefix"))
    .orderBy("merchant_prefix")
)
