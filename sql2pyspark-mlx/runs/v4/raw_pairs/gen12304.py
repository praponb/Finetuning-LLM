from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.min("amount").alias("min_amount"),
    )
)
