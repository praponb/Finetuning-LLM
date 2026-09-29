from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "txn_type")
w4 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id")
w_part = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status")
w9 = Window.partitionBy("status").orderBy("amount_bucket", "id")
w10 = Window.partitionBy("account_id").orderBy("amount_bucket", "id")
w11 = Window.partitionBy("status").orderBy("account_opened_date", "id")

enriched = (
    df_transactions
    .select(
        "*",
        F.ntile(5).over(w).alias("amount_bucket"),
        F.last("merchant", ignorenulls=True).over(w2).alias("last_known_merchant"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round((F.col("amount_bucket") / F.sum("amount_bucket").over(w3)), 4).alias("amount_bucket_share"),
        F.first("merchant", ignorenulls=True).over(w4).alias("first_known_merchant"),
        F.sum("amount").over(w5).alias("running_amount"),
    )
)

metrics = (
    with_windows
    .withColumn("cumulative_running_amount", F.sum("running_amount").over(w4))
    .withColumn("next_amount_bucket_share", F.lead("amount_bucket_share", 2, 0).over(w_ordered))
    .withColumn("cumulative_amount_bucket_share", F.sum("amount_bucket_share").over(w_part))
)

enriched_transactions = (
    metrics
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .filter(F.col("b.account_type") == "CREDIT")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.merchant",
        "src.is_flagged",
        "src.amount_bucket",
        "src.amount_bucket_share",
        "src.next_amount_bucket_share",
        F.col("b.balance"),
        F.col("b.opened_date"),
        F.col("b.account_type"),
    )
)

with_dims = (
    enriched_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.account_id",
        "e.txn_type",
        "e.amount_bucket",
        F.col("b.status"),
        F.col("b.opened_date").alias("account_opened_date"),
    )
)

enriched_transactions_2 = (
    with_dims
    .select(
        "*",
        F.avg("amount_bucket").over(w8).alias("status_avg_amount_bucket"),
        F.ntile(5).over(w9).alias("amount_bucket_bucket"),
        F.row_number().over(w10).alias("seq_no"),
        F.lag("amount_bucket", 1).over(w11).alias("prev_amount_bucket"),
    )
)

result = (
    enriched_transactions_2
    .alias("s")
    .join(df_accounts.alias("dim"), F.col("s.account_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.account_type").alias("account_type"),
        F.col("s.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.seq_no")).alias("min_seq_no"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("account_type").asc_nulls_last())
)
