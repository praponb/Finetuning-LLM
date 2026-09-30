from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.to_date("txn_time") < "2024-11-01")
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.floor("amount").alias("amount_whole"),
    )
)

result = (
    base
    .select("txn_type", "id")
    .intersect(
        df_transactions
        .filter(F.col("merchant") == "Amazon")
        .select("txn_type", "id")
    )
)
