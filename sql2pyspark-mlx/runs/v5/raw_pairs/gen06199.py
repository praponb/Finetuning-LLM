from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("balance").desc_nulls_first())
w4 = Window.orderBy(F.col("balance_rank").asc_nulls_first())
w5 = Window.partitionBy("account_id", "txn_type").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy("balance_rank_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy("balance_rank_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("txn_type").orderBy(F.col("balance_rank").desc())
w_part = Window.partitionBy("account_id").orderBy("cumulative_balance_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("balance").desc(), "id")
w11 = Window.partitionBy("txn_type").orderBy("balance", "id")
w12 = Window.orderBy("amount_avg_2", "id")
w13 = Window.partitionBy("account_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .filter(F.year(F.col("b.opened_date")) >= 2024)
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.merchant",
        F.col("b.balance"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "account_id",
        "txn_type",
        "balance",
        F.last("merchant", ignorenulls=True).over(w).alias("last_known_merchant"),
        F.avg("amount").over(w2).alias("amount_avg_2"),
        F.rank().over(w3).alias("balance_rank"),
    )
)

with_windows_transactions = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w4).alias("balance_rank_cume_dist"),
        F.avg("balance_rank").over(w5).alias("balance_rank_avg_2"),
    )
)

with_windows_transactions_2 = (
    with_windows_transactions
    .select(
        "*",
        F.sum("balance").over(w6).alias("cumulative_balance"),
        F.sum("balance").over(w7).alias("cumulative_balance_2"),
    )
)

latest = (
    with_windows_transactions_2
    .withColumn("row_num", F.rank().over(w8))
    .filter(F.col("row_num") == 1)
)

with_windows_transactions_3 = (
    latest
    .select(
        "*",
        F.sum("balance_rank_cume_dist").over(w_part).alias("running_sum_balance_rank_cume_dist"),
        F.max("balance_rank_cume_dist").over(w_part).alias("running_max_balance_rank_cume_dist"),
        F.count("balance_rank_cume_dist").over(w_part).alias("running_count_balance_rank_cume_dist"),
    )
)

scored = (
    with_windows_transactions_3
    .select(
        "id",
        "txn_type",
        "balance",
        "last_known_merchant",
        "balance_rank_avg_2",
        "running_sum_balance_rank_cume_dist",
        "running_max_balance_rank_cume_dist",
        "running_count_balance_rank_cume_dist",
        F.ntile(4).over(w10).alias("balance_bucket"),
        F.row_number().over(w11).alias("seq_no"),
        F.lag("balance_rank_avg_2", 2).over(w12).alias("prev_balance_rank_avg_2"),
        F.last("balance_rank_avg_2", ignorenulls=True).over(w13).alias("last_known_balance_rank_avg_2"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("balance_bucket") >= 7, "HIGH").otherwise("LOW").alias("balance_bucket_tier"),
        "seq_no",
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("running_count_balance_rank_cume_dist")).otherwise(0)).alias("credit_running_count_balance_rank_cume_dist"),
        F.percentile_approx("balance_rank_avg_2", 0.9).alias("balance_rank_avg_2_p90"),
        F.percentile_approx("balance_bucket", 0.95).alias("balance_bucket_p95"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
    )
)
