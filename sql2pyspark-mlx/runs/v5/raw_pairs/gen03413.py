from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("running_sum_balance").asc_nulls_first())
w_part = Window.partitionBy("status")
w4 = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w_grp = Window.partitionBy("account_type", "customer_id")
w6 = Window.partitionBy("account_type").orderBy("running_max_balance", "id")
w7 = Window.partitionBy("account_type").orderBy(F.col("running_max_balance").desc(), "id")
w8 = Window.orderBy("seq_no", "id")
w9 = Window.partitionBy("account_type").orderBy("account_type_min_running_max_balance", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("account_type")

metrics = (
    df_accounts
    .filter(F.col("balance") <= 51000)
    .select(
        "*",
        F.sum("balance").over(w_running).alias("running_sum_balance"),
        F.max("balance").over(w_running).alias("running_max_balance"),
        F.avg("balance").over(w_running).alias("running_avg_balance"),
        F.count("balance").over(w_running).alias("running_count_balance"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "running_sum_balance",
        "running_max_balance",
        "running_avg_balance",
        F.dense_rank().over(w2).alias("running_sum_balance_dense_rank"),
        F.sum("running_max_balance").over(w_part).alias("status_sum_running_max_balance"),
        F.lag("running_count_balance", 1).over(w4).alias("prev_running_count_balance"),
        F.min("running_max_balance").over(w_grp).alias("account_type_min_running_max_balance"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

scored = (
    not_flagged
    .select(
        "id",
        "account_type",
        "running_sum_balance",
        "running_avg_balance",
        "status_sum_running_max_balance",
        "prev_running_count_balance",
        "account_type_min_running_max_balance",
        F.lead("account_type_min_running_max_balance").over(w6).alias("next_account_type_min_running_max_balance"),
        F.lead("prev_running_count_balance").over(w6).alias("next_prev_running_count_balance"),
        F.lag("prev_running_count_balance").over(w6).alias("prev_prev_running_count_balance"),
        F.row_number().over(w6).alias("seq_no"),
        F.row_number().over(w7).alias("seq_no_2"),
    )
)

enriched = (
    scored
    .withColumn("next_prev_running_count_balance_2", F.lead("prev_running_count_balance", 1).over(w8))
    .withColumn("seq_no_2_avg_5", F.avg("seq_no_2").over(w9))
    .withColumn("account_type_min_status_sum_running_max_balance", F.min("status_sum_running_max_balance").over(w_hist))
)

result = (
    enriched
    .select(
        "id",
        "account_type",
        "running_sum_balance",
        "account_type_min_running_max_balance",
        "next_account_type_min_running_max_balance",
        "next_prev_running_count_balance",
        "seq_no",
        "seq_no_2",
        "next_prev_running_count_balance_2",
    )
    .orderBy(F.col("seq_no").asc_nulls_first(), "running_sum_balance")
)
