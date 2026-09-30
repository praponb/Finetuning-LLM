from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Shell") & F.col("account_id").isin([27, 40]))
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0)).alias("amazon_count"),
    )
)
