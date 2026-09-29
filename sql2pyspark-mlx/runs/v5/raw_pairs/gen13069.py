from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~(F.col("account_id").isin([14, 26, 35, 40]) & F.col("merchant").isin(["Amazon", "Starbucks", "Shell"])))
    .select("id", "txn_type", F.round("amount", 2).alias("amount_rounded"))
)
