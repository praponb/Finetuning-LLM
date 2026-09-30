from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 11, 24]))
    .select(
        "id",
        "is_flagged",
        "merchant",
        "amount",
        F.when(F.col("amount") >= 4400, "High").when(F.col("amount") >= 1800, "Medium").otherwise("Low").alias("amount_tier"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
