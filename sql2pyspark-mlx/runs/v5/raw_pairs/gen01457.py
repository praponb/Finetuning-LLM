from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-05-19"))) & ~F.col("is_flagged"))
    .select(
        "*",
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

result = (
    valid
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        "amount_rounded",
        "resolved_merchant",
        F.row_number().over(w).alias("row_seq"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
