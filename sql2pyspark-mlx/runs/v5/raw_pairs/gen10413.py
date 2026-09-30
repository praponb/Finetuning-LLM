from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2024-04-19") & F.col("is_flagged"))
    .groupBy("txn_type", "merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") >= 3400)
    .drop("min_amount")
)
