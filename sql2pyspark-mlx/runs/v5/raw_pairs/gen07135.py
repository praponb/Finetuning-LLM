from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type")
w5 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("row_seq_2").asc_nulls_last())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w9 = Window.orderBy("balance", "id")
w_running = Window.orderBy("abs_balance", "id")
w_hist = Window.orderBy(F.col("running_account_type_count_prev_balance").desc(), "id")
w12 = Window.orderBy("running_account_type_count_prev_balance", "id")
w13 = Window.orderBy("running_account_type_count_prev_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        F.lag("balance").over(w).alias("prev_balance"),
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

scored = (
    metrics
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w3))
    .withColumn("account_type_count_prev_balance", F.count("prev_balance").over(w4))
    .withColumn("prev_balance_avg_5", F.avg("prev_balance").over(w5))
)

with_windows = (
    scored
    .select(
        "id",
        "balance",
        "prev_balance",
        "row_seq",
        "row_seq_2",
        "prev_balance_avg_5",
        F.sum("account_type_count_prev_balance").over(w6).alias("running_account_type_count_prev_balance"),
        F.cume_dist().over(w7).alias("row_seq_2_cume_dist"),
    )
)

filtered = (
    with_windows
    .filter(F.col("prev_balance_avg_5").between(49127, 76119))
    .select(
        "id",
        "balance",
        "row_seq",
        "row_seq_2",
        "prev_balance_avg_5",
        "running_account_type_count_prev_balance",
        "row_seq_2_cume_dist",
        F.abs("balance").alias("abs_balance"),
        F.least("row_seq_2_cume_dist", "running_account_type_count_prev_balance").alias("least_row_seq_2_cume_dist_running_account_type_count_prev_balance"),
    )
)

valid = (
    filtered
    .filter(F.col("abs_balance") <= 38)
    .select(
        "id",
        "balance",
        "row_seq_2",
        "running_account_type_count_prev_balance",
        "abs_balance",
        F.round("least_row_seq_2_cume_dist_running_account_type_count_prev_balance", 2).alias("least_row_seq_2_cume_dist_running_account_type_count_prev_balance_rounded"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

metrics_accounts = (
    with_latest
    .select(
        "*",
        F.lead("balance", 1).over(w9).alias("next_balance"),
        F.ntile(4).over(w_running).alias("abs_balance_bucket"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.lead("amount", 1, 0).over(w12).alias("next_amount"),
        F.last("least_row_seq_2_cume_dist_running_account_type_count_prev_balance_rounded", ignorenulls=True).over(w13).alias("last_known_least_row_seq_2_cume_dist_running_account_type_count_prev_balance_rounded"),
    )
)

result = (
    metrics_accounts
    .groupBy(
        F.when(F.col("abs_balance_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("abs_balance_bucket_tier"),
    )
    .agg(
        F.avg("row_seq_2").alias("avg_row_seq_2"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
        F.count("*").alias("row_count"),
        F.round(F.avg("row_seq_2"), 2).alias("avg_row_seq_2_2"),
        F.percentile_approx("abs_balance", [0.25, 0.5, 0.75]).alias("abs_balance_quartiles"),
    )
    .orderBy(F.col("row_count").asc_nulls_first())
)
