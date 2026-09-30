from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("status")
w6 = Window.partitionBy("status").orderBy("running_min_seq_no", "id")

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("account_type").isin(["CREDIT", "LOAN", "CHECKING"]))
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.trim("status").alias("clean_status"),
        F.trim("status").alias("clean_status_2"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

with_windows = (
    not_flagged
    .filter(F.col("account_type").isin(["CHECKING", "CREDIT", "LOAN"]))
    .select(
        "id",
        "opened_date",
        "status",
        "clean_status_2",
        "status_clean",
        F.first("status", ignorenulls=True).over(w_part).alias("first_known_status"),
        F.row_number().over(w2).alias("seq_no"),
        F.first("status", ignorenulls=True).over(w_part).alias("first_known_status_2"),
        F.last("status", ignorenulls=True).over(w_part).alias("last_known_status"),
        F.last("status", ignorenulls=True).over(w3).alias("last_known_status_2"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("seq_no").over(w_recent).alias("running_avg_seq_no"),
        F.max("seq_no").over(w_recent).alias("running_max_seq_no"),
        F.min("seq_no").over(w_recent).alias("running_min_seq_no"),
        F.sum("seq_no").over(w_recent).alias("running_sum_seq_no"),
        F.sum("seq_no").over(w_running).alias("status_sum_seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "opened_date",
        "status",
        "clean_status_2",
        "status_clean",
        "last_known_status",
        "running_max_seq_no",
        "running_sum_seq_no",
        F.lead("running_avg_seq_no", 1, 0).over(w6).alias("next_running_avg_seq_no"),
        F.first("running_min_seq_no", ignorenulls=True).over(w3).alias("first_known_running_min_seq_no"),
    )
)
