from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 3400) & ((F.col("txn_time") < "2023-03-01") | F.col("is_flagged")))
    .select("txn_type", "is_flagged", "amount")
)
