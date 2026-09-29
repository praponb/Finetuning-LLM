from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 27) & (F.col("merchant") == "Shell"))
    .select("merchant", "txn_time", F.hour("txn_time").alias("txn_time_hour"))
    .orderBy("merchant", "txn_time")
)
