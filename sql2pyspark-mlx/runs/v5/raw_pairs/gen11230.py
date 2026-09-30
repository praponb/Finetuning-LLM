from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("amount") >= 2800)
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

result = (
    valid
    .groupBy("account_id")
    .pivot("merchant", ["Shell", "7-Eleven", "Starbucks"])
    .agg(F.max("amount"))
    .select(
        "account_id",
        F.col("Shell").alias("shell"),
        F.col("7-Eleven").alias("v_7_eleven"),
        F.col("Starbucks").alias("starbucks"),
    )
)
