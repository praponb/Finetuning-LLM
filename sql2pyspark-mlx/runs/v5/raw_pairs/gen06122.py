from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w5 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("status").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w8 = Window.partitionBy("status").orderBy("transaction_txn_time", "id")
w9 = Window.partitionBy("status").orderBy("transaction_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.min("balance").over(w3).alias("customer_id_min_balance"),
        F.lag("balance", 2).over(w4).alias("prev_balance"),
        F.first("status", ignorenulls=True).over(w5).alias("first_known_status"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.status",
        "s.event_seq",
        F.col("c.merchant"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

ranked = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    ranked
    .withColumn("is_new_burst", F.when(F.lag("transaction_txn_time").over(w8).isNull() | (F.col("transaction_txn_time") > F.lag("transaction_txn_time").over(w8) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_accounts = flagged.withColumn("session_no", F.sum("is_new_burst").over(w9))

summary = (
    sessions_accounts
    .groupBy("merchant", "status")
    .agg(
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("session_no"))).alias("closed_total_session_no"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
    .filter(F.col("event_seq_p50") < 1000000)
)

result = (
    summary
    .select(
        "merchant",
        "status",
        "closed_total_session_no",
        "grab_count",
        "event_seq_quartiles",
    )
)
