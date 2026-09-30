from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id")
w2 = Window.partitionBy("account_id").orderBy("balance", "id")
w3 = Window.partitionBy("txn_type").orderBy("balance", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("account_id").orderBy("prev_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy("txn_time", "id")
w7 = Window.orderBy(F.col("txn_type_sum_balance").desc_nulls_last())
w8 = Window.partitionBy("account_id").orderBy("last_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("txn_type_sum_balance_cume_dist").desc())
w_ordered = Window.partitionBy("status").orderBy("prev_balance_max_7d", "id")
w_hist = Window.partitionBy("status").orderBy(F.col("next_balance_avg_5_prior").desc(), "id")

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .filter(F.col("p.balance") < 25000)
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.txn_time",
        "s.is_flagged",
        "s.resolved_merchant",
        F.col("p.opened_date"),
        F.col("p.balance"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_time",
        "resolved_merchant",
        "opened_date",
        "balance",
        F.sum("balance").over(w).alias("txn_type_sum_balance"),
        F.lag("balance", 1).over(w2).alias("prev_balance"),
        F.avg("balance").over(w3).alias("balance_avg_5_prior"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "account_id",
        "resolved_merchant",
        "prev_balance",
        F.last("balance", ignorenulls=True).over(w4).alias("last_known_balance"),
        F.max("prev_balance").over(w5).alias("prev_balance_max_7d"),
        F.lead("balance_avg_5_prior", 2).over(w6).alias("next_balance_avg_5_prior"),
        F.cume_dist().over(w7).alias("txn_type_sum_balance_cume_dist"),
    )
)

with_windows_transactions = (
    with_windows
    .select(
        "*",
        F.sum("next_balance_avg_5_prior").over(w8).alias("cumulative_next_balance_avg_5_prior"),
        F.rank().over(w9).alias("txn_type_sum_balance_cume_dist_rank"),
    )
)

with_dims = (
    with_windows_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.status"), F.col("d.opened_date"))
)

enriched_transactions = (
    with_dims
    .select(
        "id",
        "status",
        "opened_date",
        F.lead("cumulative_next_balance_avg_5_prior").over(w_ordered).alias("next_cumulative_next_balance_avg_5_prior"),
        F.lag("cumulative_next_balance_avg_5_prior").over(w_ordered).alias("prev_cumulative_next_balance_avg_5_prior"),
        F.lead("txn_type_sum_balance_cume_dist").over(w_ordered).alias("next_txn_type_sum_balance_cume_dist"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

stats = (
    enriched_transactions
    .groupBy(F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW").alias("seq_no_tier"))
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
)

result = stats.select("seq_no_tier", "seq_no_quartiles")
