from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("merchant") != "Shell") & (F.col("merchant") == "Shell"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

summary = (
    filtered_transactions
    .groupBy("merchant", F.hour("txn_time").alias("txn_hour"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    summary
    .filter(F.col("avg_amount") > 33)
    .select("merchant", "txn_hour")
    .orderBy("merchant", "txn_hour")
    .limit(5)
)
