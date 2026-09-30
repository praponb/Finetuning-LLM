from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "7-Eleven", "Shell"]) | F.col("merchant").isin(["7-Eleven", "Shell"]))
    .withColumn("txn_type_title", F.initcap(F.trim("txn_type")))
)

agg = (
    valid_transactions
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.max("amount").alias("max_amount"),
    )
)

result = agg
