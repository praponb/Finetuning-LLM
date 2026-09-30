from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("event_time", "id")
w2 = Window.orderBy("event_seq", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy(F.col("event_seq").desc(), "id")
w4 = Window.orderBy("row_seq", "id")
w5 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy("event_seq", "id").rowsBetween(-2, -1)
w7 = Window.orderBy(F.col("row_seq_bucket").asc_nulls_last())
w8 = Window.orderBy("event_seq_2", "id")

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-05-04").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "event_time",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

scored = (
    valid
    .filter(F.hour("event_time").between(7, 13))
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

with_windows = (
    scored
    .select(
        "id",
        "page_filled",
        "row_seq",
        "event_seq",
        F.avg("event_seq").over(w2).alias("event_seq_avg_6"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.lead("event_seq", 2).over(w4).alias("next_event_seq"),
        F.ntile(3).over(w4).alias("row_seq_bucket"),
        F.avg("event_seq").over(w5).alias("event_seq_avg_7d"),
    )
)

scored_web_events = (
    with_windows
    .select(
        "*",
        F.avg("row_seq").over(w6).alias("row_seq_avg_2_prior"),
        F.cume_dist().over(w7).alias("row_seq_bucket_cume_dist"),
        F.ntile(4).over(w8).alias("event_seq_2_bucket"),
    )
)

cleaned = (
    scored_web_events
    .filter(F.col("next_event_seq") == 5)
    .withColumn("row_seq_per_row_seq_avg_2_prior", F.round((F.col("row_seq") / F.col("row_seq_avg_2_prior")), 2))
)

result = (
    cleaned
    .withColumn("event_seq_2_bucket_tier", F.when(F.col("event_seq_2_bucket") >= 6, "PREMIUM").otherwise("STANDARD"))
    .withColumn("row_seq_avg_2_prior_tier", F.when(F.col("row_seq_avg_2_prior") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["event_seq_2_bucket_tier", "row_seq_avg_2_prior_tier"], ["event_seq_2_bucket_tier"]],
        "event_seq_2_bucket_tier",
        "row_seq_avg_2_prior_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("event_seq_2").alias("total_event_seq_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("row_seq_avg_2_prior_tier").desc())
)
