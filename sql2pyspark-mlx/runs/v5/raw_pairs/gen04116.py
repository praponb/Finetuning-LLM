from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .cube(
        F.when(F.col("merchant").isin(["Amazon", "7-Eleven"]), "core").otherwise("other").alias("merchant_group"),
        "account_id",
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy("account_id")
)
