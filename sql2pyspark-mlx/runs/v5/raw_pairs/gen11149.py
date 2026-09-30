from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-2, -1)
w_recent = Window.orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w5 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("event_seq", "id")
w7 = Window.partitionBy("status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("account_type").orderBy("txn_time", "id")
w9 = Window.partitionBy("merchant", "txn_type").orderBy("amount_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("txn_type").orderBy("row_seq", "id").rowsBetween(-3, -1)
w11 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w12 = Window.orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w13 = Window.partitionBy("account_type").orderBy("event_seq_avg_90d", "id")
w14 = Window.partitionBy("account_status").orderBy(F.col("frozen_events").desc())

scored = (
    df_transactions
    .filter(F.col("amount") > 3300)
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("amount_avg_2_prior", F.avg("amount").over(w2))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w5))

with_dims = (
    sessionized
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.balance"), F.col("d.status"), F.col("d.account_type"))
)

scored_transactions = (
    with_dims
    .select(
        "*",
        F.row_number().over(w6).alias("row_seq_2"),
        F.avg("event_seq").over(w7).alias("event_seq_avg_90d"),
        F.lag("session_id", 2, 0).over(w8).alias("prev_session_id"),
    )
)

metrics = (
    scored_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "row_seq",
        "is_new_burst",
        "balance",
        "status",
        "account_type",
        F.first("event_seq", ignorenulls=True).over(w9).alias("first_known_event_seq"),
        F.avg("row_seq_2").over(w10).alias("row_seq_2_avg_3_prior"),
        F.count("is_new_burst").over(w11).alias("is_new_burst_count_90d"),
        F.avg("balance").over(w12).alias("balance_avg_6"),
        F.lag("event_seq", 2, 0).over(w13).alias("prev_event_seq"),
    )
)

with_dims_transactions = (
    metrics
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.opened_date"),
        F.col("b.balance").alias("account_balance"),
        F.col("b.status").alias("account_status"),
    )
)

result = (
    with_dims_transactions
    .groupBy(
        "account_status",
        F.when(F.col("is_new_burst_count_90d") >= 18, "HIGH").otherwise("LOW").alias("is_new_burst_count_90d_tier"),
    )
    .agg(
        F.count_if(F.col("account_status") == "FROZEN").alias("frozen_events"),
        F.count("*").alias("row_count"),
        F.avg(F.when(F.col("account_type") == "SAVINGS", F.col("account_balance"))).alias("savings_avg_account_balance"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") < 1000)
    .withColumn("frozen_events_percent_rank", F.percent_rank().over(w14))
    .orderBy(F.col("account_status").desc())
)
