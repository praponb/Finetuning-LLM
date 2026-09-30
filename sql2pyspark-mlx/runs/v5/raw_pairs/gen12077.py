from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.lower("merchant").alias("merchant_lower"),
    )
    .orderBy(F.col("id").desc())
)
