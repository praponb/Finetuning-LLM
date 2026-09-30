from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 29)
    .groupBy("merchant", "txn_type")
    .agg(F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"))
    .filter(F.col("grab_count") >= 4)
)
