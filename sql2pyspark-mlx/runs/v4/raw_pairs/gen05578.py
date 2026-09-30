from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]))
    .withColumn("txn_type_group", F.when(F.col("txn_type").isin(["DEBIT", "FEE"]), "Group A").otherwise("Other"))
)

summary = (
    filtered_transactions
    .groupBy("is_flagged")
    .agg(F.count("amount").alias("amount_count"))
)

result = summary.filter(F.col("amount_count") < 8)
