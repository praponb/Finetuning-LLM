from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2024-07-01") & ((F.col("amount") >= 1000) | (F.col("merchant") == "Grab")))
    .select(
        "id",
        "txn_type",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)
