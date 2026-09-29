from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2024-03-15 03:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.when(F.col("amount") >= 3100, "High").when(F.col("amount") >= 1200, "Medium").otherwise("Low").alias("amount_level"),
        F.date_trunc("YEAR", "txn_time").alias("txn_time_period"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

result = (
    filtered
    .groupBy("account_id")
    .pivot("merchant", ["Grab", "Lazada"])
    .agg(F.max("amount").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "account_id",
        F.col("Grab_total").alias("grab_total"),
        F.col("Grab_n").alias("grab_n"),
        F.col("Lazada_total").alias("lazada_total"),
        F.col("Lazada_n").alias("lazada_n"),
    )
)
