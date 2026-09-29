from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w_recent = Window.partitionBy("account_id", "status").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("event_seq", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.txn_time",
        F.col("d.balance"),
        F.col("d.account_type"),
        F.col("d.status"),
    )
)

cleaned = (
    enriched
    .filter((F.col("txn_time") > F.lit("2023-07-06").cast("date")) & F.col("account_type").isNotNull())
    .select(
        "*",
        F.concat("status", F.lit(" "), "account_type").alias("status_account_type"),
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select("e.*", F.col("a.opened_date"))
)

enriched_transactions = (
    with_dims
    .select(
        "id",
        "days_since_txn_time",
        "opened_date",
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w_recent).alias("event_seq_2"),
        F.first("days_since_txn_time", ignorenulls=True).over(w3).alias("first_known_days_since_txn_time"),
    )
)

enriched_transactions_2 = (
    enriched_transactions
    .select(
        "id",
        "days_since_txn_time",
        "opened_date",
        F.min("event_seq").over(w_part).alias("last7_min_event_seq"),
        F.sum("event_seq").over(w_part).alias("last7_sum_event_seq"),
        F.avg("event_seq").over(w_part).alias("last7_avg_event_seq"),
    )
)

result = (
    enriched_transactions_2
    .withColumn("last7_min_event_seq_tier", F.when(F.col("last7_min_event_seq") >= 7.4, "HIGH").otherwise("LOW"))
    .withColumn("last7_avg_event_seq_tier", F.when(F.col("last7_avg_event_seq") >= 7.8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["last7_min_event_seq_tier", "last7_avg_event_seq_tier"], ["last7_min_event_seq_tier"], []],
        "last7_min_event_seq_tier",
        "last7_avg_event_seq_tier",
    )
    .agg(
        F.sum("days_since_txn_time").alias("total_days_since_txn_time"),
        F.percentile_approx("last7_avg_event_seq", 0.95).alias("last7_avg_event_seq_p95"),
        F.percentile_approx("last7_sum_event_seq", 0.95).alias("last7_sum_event_seq_p95"),
        F.grouping("last7_min_event_seq_tier").alias("grouping_level"),
    )
    .orderBy(F.col("last7_avg_event_seq_p95").asc_nulls_last())
)
