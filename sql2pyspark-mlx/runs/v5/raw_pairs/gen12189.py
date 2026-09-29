from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy(F.col("seq_no").desc_nulls_first())
w3 = Window.partitionBy("merchant", "status").orderBy("seq_no", "id")
w4 = Window.partitionBy("account_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("txn_type").isin(["REFUND", "FEE", "CREDIT"]))
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

with_windows = (
    filtered
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.lag("amount").over(w).alias("prev_amount"),
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("seq_no"),
    )
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.account_type"), F.col("p.status"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select(
        "a.id",
        "a.account_id",
        "a.merchant",
        "a.is_flagged",
        "a.prev_amount",
        "a.seq_no",
        "a.status",
        F.col("p2.opened_date"),
    )
)

scored = (
    joined
    .select(
        "id",
        "account_id",
        "merchant",
        "prev_amount",
        "seq_no",
        "status",
        F.dense_rank().over(w2).alias("seq_no_dense_rank"),
        F.ntile(3).over(w3).alias("seq_no_bucket"),
        F.sum("prev_amount").over(w4).alias("running_prev_amount"),
    )
)

result = (
    scored
    .select("id", "merchant", "prev_amount", "seq_no_bucket", "running_prev_amount")
    .orderBy(F.col("prev_amount").desc_nulls_last())
)
