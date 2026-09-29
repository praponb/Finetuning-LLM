from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
    .filter(F.col("starbucks_count") > 74)
    .orderBy(F.col("starbucks_count").desc(), F.col("account_id").desc())
    .limit(20)
)
