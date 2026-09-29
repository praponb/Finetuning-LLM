from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("txn_time").desc(), "id")
w5 = Window.partitionBy("account_id").orderBy(F.col("event_seq_rounded").desc(), "id")

enriched = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_time",
        "s.merchant",
        F.col("p.opened_date"),
        F.col("p.status"),
    )
)

with_windows = (
    enriched
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w3))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    deduped
    .filter(F.col("txn_time") > F.lit("2023-08-28 23:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "merchant",
        "opened_date",
        "status",
        "event_seq",
        "first_known_status",
        F.round("event_seq", 2).alias("event_seq_rounded"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "account_id",
        "merchant",
        "status",
        "event_seq",
        "first_known_status",
        "event_seq_rounded",
        "resolved_status",
    )
)

result = (
    latest
    .withColumn("event_seq_rounded_tier", F.when(F.col("event_seq_rounded") >= 64, "HIGH").otherwise("LOW"))
    .rollup("status", "event_seq_rounded_tier")
    .agg(
        F.min("event_seq").alias("min_event_seq"),
        F.round(F.avg("event_seq_rounded"), 2).alias("avg_event_seq_rounded"),
        F.percentile_approx("event_seq", 0.95).alias("event_seq_p95"),
        F.grouping("status").alias("gid"),
    )
    .orderBy("event_seq_rounded_tier")
)
