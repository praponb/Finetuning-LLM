from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount")
w_recent = Window.partitionBy("merchant").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type", "merchant").orderBy("rn", "id")
w4 = Window.partitionBy("merchant").orderBy("running_sum_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w8 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_transactions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.sum("amount").over(w_recent).alias("running_sum_amount"),
        F.avg("amount").over(w_recent).alias("running_avg_amount"),
        F.max("amount").over(w_recent).alias("running_max_amount"),
    )
)

scored = (
    with_windows
    .withColumn("prev_running_avg_amount", F.lag("running_avg_amount", 1).over(w3))
    .withColumn("cumulative_running_sum_amount", F.sum("running_sum_amount").over(w4))
    .withColumn("running_avg_amount_max_30d", F.max("running_avg_amount").over(w5))
    .withColumn("running_avg_amount_max_7d", F.max("running_avg_amount").over(w6))
)

gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w7) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("session_start_flag").over(w8))

enriched = (
    sessions_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "inner")
    .filter(F.col("b.balance").isNotNull())
    .select("e.*", F.col("b.balance"), F.col("b.status"), F.col("b.opened_date"))
)

cleaned = (
    enriched
    .filter((F.col("txn_time") >= F.lit("2023-11-07").cast("date")) & (F.col("status") == "CLOSED"))
    .select(
        "*",
        F.ceil("running_avg_amount").alias("running_avg_amount_whole"),
        (F.col("running_max_amount") * F.col("session_start_flag")).alias("running_max_amount_times_session_start_flag"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.status").alias("account_status"),
        F.col("d.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("a.session_no")), 2).alias("avg_session_no"),
    )
    .orderBy("account_status")
)
