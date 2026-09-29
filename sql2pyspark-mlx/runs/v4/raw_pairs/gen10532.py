from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((~F.col("is_flagged") & ~F.col("is_flagged")) | (F.col("txn_time") < "2025-01-01"))
    .select("id", "txn_type", F.ceil("amount").alias("amount_whole"))
)
