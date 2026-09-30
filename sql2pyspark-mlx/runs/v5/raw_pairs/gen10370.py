from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("account_id").isin([37, 38]))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.round("amount", 1).alias("amount_rounded"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

result = (
    cleaned
    .select("id", "merchant", "amount_rounded", "amount")
    .unpivot(
        ids=["id", "merchant"],
        values=["amount_rounded", "amount"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
