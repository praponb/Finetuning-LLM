from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("page").orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("web_event_duration_sec", "id")
w_ordered = Window.partitionBy("page").orderBy("web_event_event_time", "id")
w5 = Window.partitionBy("page")

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.trim("browser").alias("clean_browser"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_time"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.clean_browser",
        "s.web_event_duration_sec",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.page"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "clean_browser",
        "web_event_duration_sec",
        "web_event_event_time",
        "page",
        F.min("web_event_duration_sec").over(w_hist).alias("running_min_web_event_duration_sec"),
        F.sum("web_event_duration_sec").over(w_hist).alias("running_sum_web_event_duration_sec"),
        F.avg("web_event_duration_sec").over(w_hist).alias("running_avg_web_event_duration_sec"),
        F.count("web_event_duration_sec").over(w_hist).alias("running_count_web_event_duration_sec"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "web_event_event_time",
        F.lag("running_count_web_event_duration_sec").over(w_ordered).alias("prev_running_count_web_event_duration_sec"),
        F.lead("row_seq").over(w_ordered).alias("next_row_seq"),
        F.lag("row_seq").over(w_ordered).alias("prev_row_seq"),
        F.row_number().over(w_ordered).alias("row_seq_2"),
        F.avg("running_count_web_event_duration_sec").over(w5).alias("page_avg_running_count_web_event_duration_sec"),
    )
)
