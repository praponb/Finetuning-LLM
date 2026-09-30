from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("row_seq").desc(), "id")
w3 = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("customer_id")
w6 = Window.partitionBy("status").orderBy("opened_date", "id")
w7 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w8 = Window.partitionBy("customer_id").orderBy("seq_no", "id")
w9 = Window.partitionBy("status").orderBy(F.col("event_seq").asc_nulls_last())

base = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
)

with_windows = base.select("*", F.row_number().over(w).alias("row_seq"))

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
        F.avg("row_seq").over(w4).alias("row_seq_avg_5"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w5)), 4).alias("row_seq_share"),
        F.lead("row_seq", 2).over(w6).alias("next_row_seq"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

with_windows_accounts = (
    ranked
    .withColumn("next_seq_no", F.lead("seq_no").over(w8))
    .withColumn("prev_seq_no", F.lag("seq_no").over(w8))
    .withColumn("next_row_seq_share", F.lead("row_seq_share").over(w8))
    .withColumn("prev_row_seq_share", F.lag("row_seq_share").over(w8))
    .withColumn("row_seq_2", F.row_number().over(w8))
    .withColumn("event_seq_dense_rank", F.dense_rank().over(w9))
)

result = (
    with_windows_accounts
    .groupBy(
        F.when(F.col("row_seq_share") >= 0.82, "PREMIUM").otherwise("STANDARD").alias("row_seq_share_tier"),
    )
    .agg(
        F.max(F.when(F.col("status") == "CLOSED", F.col("prev_row_seq_share"))).alias("closed_max_prev_row_seq_share"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("row_count").asc_nulls_first())
)
