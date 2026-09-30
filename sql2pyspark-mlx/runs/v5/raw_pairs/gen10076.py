from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("account_id").orderBy("txn_time", "id")
w4 = Window.partitionBy("status").orderBy(F.col("last3_count_amount").desc())
w5 = Window.partitionBy("account_id").orderBy("last3_count_amount_percent_rank", "id").rowsBetween(-2, Window.currentRow)

scored = (
    df_transactions
    .select(
        "*",
        F.max("amount").over(w).alias("last3_max_amount"),
        F.sum("amount").over(w).alias("last3_sum_amount"),
        F.count("amount").over(w).alias("last3_count_amount"),
        F.last("merchant", ignorenulls=True).over(w2).alias("last_known_merchant"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.txn_time",
        "a.merchant",
        "a.last3_max_amount",
        "a.last3_count_amount",
        "a.last_known_merchant",
        F.col("p2.balance"),
        F.col("p2.opened_date"),
        F.col("p2.status"),
    )
)

with_dims_transactions = (
    with_dims
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .filter(F.month(F.col("a.opened_date")).isin([1, 9, 12]))
    .select(
        "e.id",
        "e.account_id",
        "e.amount",
        "e.txn_time",
        "e.last3_count_amount",
        "e.last_known_merchant",
        "e.status",
        F.col("a.account_type"),
        F.col("a.opened_date").alias("account_opened_date"),
    )
)

with_windows = (
    with_dims_transactions
    .select(
        "*",
        F.lead("amount", 2).over(w_hist).alias("next_amount"),
        F.percent_rank().over(w4).alias("last3_count_amount_percent_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "last3_count_amount",
        "status",
        "account_type",
        "next_amount",
        "last3_count_amount_percent_rank",
        F.min("last3_count_amount_percent_rank").over(w5).alias("last3_min_last3_count_amount_percent_rank"),
        F.sum("last3_count_amount_percent_rank").over(w5).alias("last3_sum_last3_count_amount_percent_rank"),
        F.max("last3_count_amount_percent_rank").over(w5).alias("last3_max_last3_count_amount_percent_rank"),
        F.avg("last3_count_amount_percent_rank").over(w5).alias("last3_avg_last3_count_amount_percent_rank"),
    )
)

result = (
    metrics
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .groupingSets(
        [["account_status", "transaction_account_type"], ["account_status"]],
        F.col("d.status").alias("account_status"),
        F.col("src.account_type").alias("transaction_account_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.last3_avg_last3_count_amount_percent_rank")), 2).alias("total_last3_avg_last3_count_amount_percent_rank"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
