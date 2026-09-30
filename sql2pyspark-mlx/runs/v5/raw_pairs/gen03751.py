from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w2 = Window.partitionBy("account_id").orderBy(F.col("balance").asc_nulls_first())
w3 = Window.partitionBy("account_id").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("account_type", "account_id").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("opened_date", "id")
w6 = Window.partitionBy("account_id").orderBy("account_type_count_balance", "id")
w7 = Window.partitionBy("account_id").orderBy("next_balance", "id")
w8 = Window.partitionBy("account_id").orderBy(F.col("balance_rank").desc(), "id")
w9 = Window.partitionBy("account_id").orderBy(F.col("seq_no").desc_nulls_last())
w_part = Window.orderBy(F.col("balance").desc_nulls_last())
w11 = Window.partitionBy("account_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("account_id").orderBy("row_seq", "id")
w13 = Window.orderBy(F.col("seq_no").desc(), "id")
w14 = Window.orderBy("seq_no", "id")
w15 = Window.orderBy(F.col("seq_no_dense_rank").desc(), "id")
w16 = Window.orderBy("first_known_balance_rank", "id")
w17 = Window.orderBy(F.col("row_seq").desc(), "id")
w_running = Window.orderBy("balance_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.txn_type",
        F.col("d.opened_date"),
        F.col("d.balance"),
        F.col("d.account_type"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "account_id",
        "txn_type",
        "opened_date",
        "balance",
        "account_type",
        F.count("balance").over(w).alias("account_type_count_balance"),
        F.rank().over(w2).alias("balance_rank"),
        F.avg("balance").over(w3).alias("balance_avg_5"),
        F.avg("balance").over(w4).alias("balance_avg_2"),
        F.lead("balance", 2).over(w5).alias("next_balance"),
    )
)

metrics = (
    with_windows
    .filter(F.col("balance_avg_5") < 62144)
    .select(
        "id",
        "account_id",
        "opened_date",
        "balance",
        "balance_rank",
        "balance_avg_5",
        "balance_avg_2",
        "next_balance",
        F.row_number().over(w6).alias("seq_no"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "account_id",
        "opened_date",
        "balance",
        "balance_rank",
        "seq_no",
        "row_seq",
    )
)

with_windows_transactions = (
    ranked
    .select(
        "id",
        "opened_date",
        "balance_rank",
        "seq_no",
        "row_seq",
        F.dense_rank().over(w9).alias("seq_no_dense_rank"),
        F.cume_dist().over(w_part).alias("balance_cume_dist"),
        F.first("balance_rank", ignorenulls=True).over(w11).alias("first_known_balance_rank"),
        F.row_number().over(w12).alias("seq_no_2"),
    )
)

enriched = (
    with_windows_transactions
    .select(
        "*",
        F.row_number().over(w13).alias("row_seq_2"),
        F.lag("balance_cume_dist", 2, 0).over(w14).alias("prev_balance_cume_dist"),
    )
)

with_windows_transactions_2 = (
    enriched
    .select(
        "id",
        "balance_rank",
        "seq_no_dense_rank",
        "balance_cume_dist",
        "prev_balance_cume_dist",
        F.ntile(10).over(w15).alias("seq_no_dense_rank_bucket"),
        F.lag("seq_no", 1).over(w16).alias("prev_seq_no"),
        F.ntile(3).over(w17).alias("row_seq_bucket"),
        F.sum("seq_no_2").over(w_running).alias("running_seq_no_2"),
        F.ntile(5).over(w13).alias("seq_no_bucket"),
    )
)

result = (
    with_windows_transactions_2
    .groupBy(
        F.when(F.col("prev_balance_cume_dist") >= 0.64, "LARGE").otherwise("SMALL").alias("prev_balance_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("seq_no_dense_rank_bucket"), 2).alias("avg_seq_no_dense_rank_bucket"),
        F.percentile_approx("running_seq_no_2", [0.25, 0.5, 0.75]).alias("running_seq_no_2_quartiles"),
        F.min("balance_cume_dist").alias("min_balance_cume_dist"),
    )
    .filter(F.col("avg_seq_no_dense_rank_bucket") > 0)
    .orderBy("prev_balance_cume_dist_tier")
)
