from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~(F.col("account_id").isin([2, 7, 14, 28]) & F.col("merchant").isin(["Amazon", "Starbucks"])))
    .select("amount", "txn_time", "is_flagged")
)
