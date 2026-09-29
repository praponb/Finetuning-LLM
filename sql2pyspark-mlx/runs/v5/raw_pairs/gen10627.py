from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "status").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.partitionBy("status").orderBy("txn_time", "id")
w3 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("txn_time_date", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("last7_count_amount", "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-08-21").cast("date")) & ~F.col("is_flagged"))
    .withColumn("txn_time_date", F.to_date("txn_time"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type") != "CHECKING")
    .select("s.*", F.col("a.account_type"), F.col("a.opened_date"), F.col("a.status"))
)

ranked = (
    with_dims
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

flagged = (
    ranked
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "txn_time_date",
        "account_type",
        "opened_date",
        "status",
        "rn",
        F.sum("session_start_flag").over(w3).alias("session_id"),
    )
)

metrics = (
    islands
    .withColumn("last7_avg_amount", F.avg("amount").over(w4))
    .withColumn("last7_count_amount", F.count("amount").over(w4))
    .withColumn("last7_max_amount", F.max("amount").over(w4))
    .withColumn("last7_min_amount", F.min("amount").over(w4))
)

scored = (
    metrics
    .withColumn("prev_last7_min_amount", F.lag("last7_min_amount").over(w5))
    .withColumn("next_last7_min_amount", F.lead("last7_min_amount").over(w5))
    .withColumn("next_last7_avg_amount", F.lead("last7_avg_amount").over(w5))
    .withColumn("event_seq", F.row_number().over(w5))
)

gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

result = (
    sessions_transactions
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .cube(
        F.col("d.status").alias("account_status"),
        F.col("src.status").alias("transaction_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.event_seq")).alias("min_event_seq"),
        F.sum(F.col("src.next_last7_min_amount")).alias("total_next_last7_min_amount"),
    )
)
