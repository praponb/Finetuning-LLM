from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Shell") & F.col("account_id").isin([13, 40]))
    .groupBy("is_flagged")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
    )
    .orderBy("pct_lazada")
)
