from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")
w3 = Window.partitionBy("status").orderBy("event_seq", "id")
w4 = Window.partitionBy("status").orderBy("paid_at_date", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("paid_at", "id")
w_ordered = Window.partitionBy("status", "method").orderBy(F.col("event_seq").asc_nulls_last())
w7 = Window.partitionBy("status").orderBy(F.col("event_seq").desc())
w8 = Window.partitionBy("status", "method").orderBy("next_event_seq_2", "id")
w9 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w10 = Window.partitionBy("method").orderBy(F.col("event_seq_rank").desc(), "id")
w11 = Window.partitionBy("method", "status").orderBy(F.col("event_seq_rank").asc_nulls_last(), "id")
w12 = Window.partitionBy("status").orderBy(F.col("event_seq_per_failed").desc())

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-07-26").cast("date")) & F.col("amount").between(600, 760))
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.first("method", ignorenulls=True).over(w).alias("first_known_method"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .filter(F.hour("paid_at").between(11, 15))
    .select(
        "*",
        F.lead("event_seq", 1).over(w3).alias("next_event_seq"),
        F.avg("event_seq").over(w4).alias("event_seq_avg_6_prior"),
        F.lead("event_seq", 1).over(w5).alias("next_event_seq_2"),
        F.rank().over(w_ordered).alias("event_seq_rank"),
        F.dense_rank().over(w7).alias("event_seq_dense_rank"),
    )
)

with_windows_payments = (
    enriched
    .withColumn("next_event_seq_2_bucket", F.ntile(5).over(w8))
    .withColumn("next_event_seq_2_count_90d", F.count("next_event_seq_2").over(w9))
    .withColumn("event_seq_2", F.row_number().over(w10))
)

deduped = (
    with_windows_payments
    .withColumn("row_num", F.row_number().over(w11))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "paid_at",
        "status",
        "paid_at_date",
        "event_seq",
        "event_seq_avg_6_prior",
        "next_event_seq_2",
        "event_seq_rank",
        "next_event_seq_2_bucket",
    )
)

result = (
    deduped
    .groupBy("status", F.date_trunc("QUARTER", "paid_at_date").alias("quarter_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("event_seq_rank", [0.25, 0.5, 0.75]).alias("event_seq_rank_quartiles"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("event_seq_per_failed"),
    )
    .filter(F.col("event_seq_per_failed") > 0)
    .withColumn("event_seq_per_failed_percent_rank", F.percent_rank().over(w12))
)
