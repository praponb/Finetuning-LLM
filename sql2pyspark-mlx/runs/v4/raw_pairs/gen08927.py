from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "account_id",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.when(F.col("amount") >= 3900, "High").when(F.col("amount") >= 1100, "Medium").otherwise("Low").alias("amount_level"),
    )
    .orderBy(F.col("account_id").desc(), "amount_level")
)
