from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 16, 31, 39]))
    .groupBy("merchant")
    .agg(
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("starbucks_count").desc())
)
