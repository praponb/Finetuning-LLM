from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount_rounded", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy(F.col("amount_rounded").desc_nulls_last())

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= F.to_date(F.lit("2023-06-12"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.round("amount", 1).alias("amount_rounded"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_rounded",
        "resolved_merchant",
        F.avg("amount").over(w).alias("amount_avg_5"),
        F.cume_dist().over(w2).alias("amount_rounded_cume_dist"),
    )
)
