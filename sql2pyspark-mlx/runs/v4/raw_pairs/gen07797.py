from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
    )
    .filter(F.col("amazon_count") > 3)
)
