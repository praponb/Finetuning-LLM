from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & ((F.col("merchant") == "Amazon") | (F.col("merchant") == "Starbucks")))
    .select("id", "txn_type", "account_id")
    .orderBy("account_id")
)
