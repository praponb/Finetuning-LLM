from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("amount") >= 1000))
    .select("id", "is_flagged", "txn_time")
)
