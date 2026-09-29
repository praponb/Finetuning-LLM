from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 7)
    .select("id", "account_id", "is_flagged", "merchant")
)
