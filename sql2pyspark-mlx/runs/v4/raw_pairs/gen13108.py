from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-02-16")
    .withColumn("clean_txn_type", F.trim("txn_type"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)
