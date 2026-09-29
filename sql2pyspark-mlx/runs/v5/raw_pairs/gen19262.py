from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time_hour", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("page").orderBy("duration_sec_avg_3_prior")
w_recent = Window.orderBy(F.col("duration_sec_avg_3_prior").desc(), "id")
w6 = Window.partitionBy("event_type").orderBy("duration_sec_avg_3_prior_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    latest
    .filter((F.col("event_time") > F.lit("2023-06-04").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "latest_rank",
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

scored = (
    valid
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "latest_rank",
        "event_time_hour",
        "page_filled",
        F.sum("event_time_hour").over(w2).alias("running_event_time_hour"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_3_prior"),
    )
)

scored_web_events = (
    scored
    .filter(F.col("event_type") != "purchase")
    .withColumn("duration_sec_avg_3_prior_rank", F.rank().over(w4))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

with_windows = (
    scored_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page_filled",
        "running_event_time_hour",
        "duration_sec_avg_3_prior_rank",
        "row_seq",
        F.max("row_seq").over(w6).alias("running_max_row_seq"),
        F.count("row_seq").over(w6).alias("running_count_row_seq"),
        F.avg("row_seq").over(w6).alias("running_avg_row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "session_id",
        "event_type",
        "page_filled",
        "running_event_time_hour",
        "running_avg_row_seq",
        F.when(F.col("event_type").rlike("^(add_to_cart|click)$"), "EXTENDED").when(F.col("event_type").rlike("^(page_view|purchase)$"), "GROUP_A").otherwise("GROUP_B").alias("event_type_zone"),
    )
)
