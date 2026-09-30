from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")
w_ordered = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w3 = Window.partitionBy("plan").orderBy("row_seq", "id")
w4 = Window.partitionBy("status").orderBy(F.col("row_seq_2").desc())
w5 = Window.orderBy(F.col("event_seq").asc_nulls_last())
w6 = Window.partitionBy("plan", "status").orderBy(F.col("row_seq_2_cume_dist").asc_nulls_first(), "id")
w7 = Window.partitionBy("plan").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("status")
w9 = Window.partitionBy("plan").orderBy("row_seq_2_cume_dist", "id")
w10 = Window.partitionBy("status").orderBy("row_seq_share", "id")
w11 = Window.orderBy("end_date", "id")
w12 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("plan").orderBy(F.col("row_seq_bucket_2").desc())
w14 = Window.orderBy(F.col("row_seq").desc(), "id")
w15 = Window.partitionBy("status").orderBy("rn_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("plan").orderBy(F.col("row_seq_bucket_2").desc(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 220)
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

scored = (
    filtered
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("row_seq_2", F.row_number().over(w_ordered))
)

enriched = (
    scored
    .withColumn("row_seq_bucket", F.ntile(3).over(w3))
    .withColumn("row_seq_2_cume_dist", F.cume_dist().over(w4))
    .withColumn("event_seq_cume_dist", F.cume_dist().over(w5))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.ntile(3).over(w7).alias("row_seq_bucket_2"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w8)), 4).alias("row_seq_share"),
        F.lead("event_seq", 1).over(w9).alias("next_event_seq"),
    )
)

with_windows_subscriptions = (
    with_windows
    .withColumn("prev_row_seq_2", F.lag("row_seq_2", 2).over(w10))
    .withColumn("next_next_event_seq", F.lead("next_event_seq", 2, 0).over(w11))
    .withColumn("cumulative_row_seq_2_cume_dist", F.sum("row_seq_2_cume_dist").over(w12))
)

latest_subscriptions = (
    with_windows_subscriptions
    .withColumn("rn_2", F.rank().over(w13))
    .filter(F.col("rn_2") == 1)
)

result = (
    latest_subscriptions
    .select(
        "*",
        F.ntile(5).over(w14).alias("row_seq_bucket_3"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w8)), 4).alias("event_seq_share"),
        F.sum("event_seq").over(w15).alias("running_event_seq"),
        F.ntile(3).over(w_grp).alias("row_seq_bucket_2_bucket"),
    )
)
