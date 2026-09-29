from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .select("id", "merchant", "is_flagged", "account_id")
    .orderBy("account_id", "merchant")
)
