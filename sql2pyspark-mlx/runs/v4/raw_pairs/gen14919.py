from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2024-04-19").cast("date"))
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.when(F.col("merchant").isin(["Amazon", "Grab"]), "Priority").otherwise("Other").alias("merchant_group"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

result = (
    valid
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "Starbucks"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
