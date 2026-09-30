from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") | (F.col("txn_type") == "CREDIT"))
    .select("id", "txn_time", F.to_date("txn_time").alias("txn_time_date"))
)
