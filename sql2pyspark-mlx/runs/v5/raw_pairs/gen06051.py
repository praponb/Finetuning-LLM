from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Starbucks"]))
    .select(
        "id",
        "amount",
        "merchant",
        "account_id",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("merchant")
)
