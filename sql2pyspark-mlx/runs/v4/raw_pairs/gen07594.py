from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id")
w4 = Window.partitionBy("txn_type")
w5 = Window.partitionBy("account_id").orderBy("amount", "id")

with_windows = (
    df_transactions
    .select(
        "*",
        F.avg("amount").over(w_recent).alias("amount_avg_6_prior"),
        F.sum("amount").over(w2).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share_2"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "running_amount",
        "amount_share",
        "amount_share_2",
        "event_seq",
    )
)
