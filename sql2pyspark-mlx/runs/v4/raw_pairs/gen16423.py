from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
    .filter(F.col("starbucks_count") > 66)
)
