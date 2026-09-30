from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNull())
    .select(
        "id",
        "is_flagged",
        F.coalesce("merchant", F.lit("Unknown")).alias("merchant_filled"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("is_flagged_label")
)
