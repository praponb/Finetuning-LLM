from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 22, 38]))
    .select(
        "id",
        "merchant",
        "amount",
        F.when(F.col("amount") >= 4300, "High").when(F.col("amount") >= 1600, "Medium").otherwise("Low").alias("amount_tier"),
        F.initcap(F.trim("merchant")).alias("merchant_title"),
    )
)
