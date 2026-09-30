from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~(F.col("account_id").isin([4, 30]) & (F.col("merchant") == "Amazon")))
    .select(
        "id",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy(F.col("id").desc())
)
