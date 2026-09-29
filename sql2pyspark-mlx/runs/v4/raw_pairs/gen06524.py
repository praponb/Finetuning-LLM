from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("running_balance").desc_nulls_last())
w_recent = Window.partitionBy("customer_id").orderBy("running_balance", "id")
w_ordered = Window.partitionBy("status").orderBy(F.col("balance_avg_3").desc(), "id")
w6 = Window.partitionBy("account_type", "status").orderBy("opened_date", "id")
w7 = Window.partitionBy("account_type").orderBy("prev_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("balance", "id")
w_running = Window.partitionBy("customer_id", "status").orderBy("balance_avg_3", "id")
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_windows = (
    df_accounts
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("balance_avg_3", F.avg("balance").over(w2))
)

filtered = (
    with_windows
    .filter(F.col("customer_id").isNotNull())
    .withColumn("status_lower", F.lower("status"))
    .withColumn("status_clean", F.coalesce("status", F.lit("DIRECT")))
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest
    .select(
        "*",
        F.lag("balance", 2).over(w_recent).alias("prev_balance"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

with_windows_accounts = (
    scored
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w6))
    .withColumn("last_known_running_balance", F.last("running_balance", ignorenulls=True).over(w7))
    .withColumn("balance_bucket", F.ntile(5).over(w8))
    .withColumn("next_running_balance", F.lead("running_balance", 1, 0).over(w_running))
)

with_last = (
    with_windows_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

result = (
    with_last
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("next_running_balance")).otherwise(0)).alias("loan_next_running_balance"),
        F.max("prev_balance").alias("max_prev_balance"),
        F.round(F.avg("last_known_running_balance"), 2).alias("avg_last_known_running_balance"),
        F.percentile_approx("running_balance", [0.25, 0.5, 0.75]).alias("running_balance_quartiles"),
    )
    .filter(F.col("max_prev_balance") < 0)
)
