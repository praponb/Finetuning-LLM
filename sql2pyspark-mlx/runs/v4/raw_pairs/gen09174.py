from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2024-05-19 02:00:00"))) & F.hour("txn_time").between(10, 17))
    .select(
        "*",
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"),
        F.coalesce("merchant", F.lit("unassigned")).alias("merchant_filled"),
    )
)

result = (
    cleaned
    .select("account_id")
    .intersect(
        df_transactions
        .filter(F.col("txn_type").isin(["FEE", "REFUND"]))
        .select("account_id")
    )
)
