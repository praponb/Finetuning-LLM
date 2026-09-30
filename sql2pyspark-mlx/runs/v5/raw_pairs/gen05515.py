from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date_plus_months", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("opened_date_plus_months").desc(), "id")
w3 = Window.orderBy("opened_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("account_type").orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w7 = Window.orderBy("transaction_amount", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy(F.col("first_known_event_seq").desc_nulls_last())
w9 = Window.orderBy(F.col("transaction_amount").desc(), "id")
w10 = Window.orderBy("event_seq", "id")
w11 = Window.orderBy("event_seq_2_avg_3_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy(F.col("event_seq_bucket").desc())
w13 = Window.orderBy(F.col("event_seq").asc_nulls_first())

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

with_windows = (
    valid
    .filter(F.col("opened_date_plus_months") > "2024-11-21")
    .select(
        "id",
        "account_type",
        "opened_date_plus_months",
        "status_clean",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
    )
)

enriched = (
    with_windows
    .withColumn("first_known_event_seq", F.first("event_seq", ignorenulls=True).over(w3))
    .withColumn("event_seq_2_avg_3", F.avg("event_seq_2").over(w4))
    .withColumn("cumulative_event_seq", F.sum("event_seq").over(w_hist))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date_plus_months")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date_plus_months",
        "s.event_seq",
        "s.event_seq_2",
        "s.first_known_event_seq",
        "s.event_seq_2_avg_3",
        F.col("c.amount").alias("transaction_amount"),
    )
)

enriched_accounts = (
    with_latest
    .select(
        "id",
        "opened_date_plus_months",
        "event_seq",
        "event_seq_2",
        F.avg("event_seq_2_avg_3").over(w7).alias("event_seq_2_avg_3_avg_6"),
        F.cume_dist().over(w8).alias("first_known_event_seq_cume_dist"),
        F.row_number().over(w9).alias("event_seq_3"),
        F.ntile(5).over(w10).alias("event_seq_bucket"),
    )
)

result = (
    enriched_accounts
    .withColumn("last_known_event_seq_2", F.last("event_seq_2", ignorenulls=True).over(w11))
    .withColumn("event_seq_bucket_cume_dist", F.cume_dist().over(w12))
    .withColumn("event_seq_percent_rank", F.percent_rank().over(w13))
)
