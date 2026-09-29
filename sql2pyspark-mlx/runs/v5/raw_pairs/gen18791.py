from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w7 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("row_seq").desc(), "id")
w9 = Window.partitionBy("browser")
w10 = Window.partitionBy("browser").orderBy("seq_no", "id")
w11 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_recent = Window.partitionBy("country").orderBy("next_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("web_event_page").orderBy("first_known_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id").orderBy(F.col("web_event_duration_sec").desc(), "id")
w15 = Window.partitionBy("customer_id", "browser").orderBy("seq_no_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w16 = Window.partitionBy("browser", "country").orderBy("first_known_web_event_duration_sec", "id").rowsBetween(-6, Window.currentRow)
w17 = Window.partitionBy("browser").orderBy("web_event_duration_sec_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.page").alias("web_event_page"),
    )
)

scored = (
    with_latest
    .filter(F.col("web_event_duration_sec") != 444)
    .select(
        "*",
        F.first("seq_no", ignorenulls=True).over(w7).alias("first_known_seq_no"),
        F.ntile(3).over(w8).alias("row_seq_bucket"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w9)), 4).alias("seq_no_share"),
        F.lead("seq_no", 1).over(w10).alias("next_seq_no"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "browser",
        "country",
        "first_known_browser",
        "row_seq",
        "web_event_page",
        "first_known_seq_no",
        "seq_no_share",
        F.count("next_seq_no").over(w11).alias("next_seq_no_count_30d"),
        F.sum("event_seq").over(w_recent).alias("running_event_seq"),
        F.last("web_event_duration_sec", ignorenulls=True).over(w13).alias("last_known_web_event_duration_sec"),
        F.ntile(5).over(w_grp).alias("web_event_duration_sec_bucket"),
        F.first("web_event_duration_sec", ignorenulls=True).over(w15).alias("first_known_web_event_duration_sec"),
    )
)

scored_sessions = (
    with_windows
    .withColumn("first_known_seq_no_avg_6", F.avg("first_known_seq_no").over(w16))
    .withColumn("cumulative_first_known_seq_no", F.sum("first_known_seq_no").over(w17))
)

result = (
    scored_sessions
    .select(
        "id",
        "browser",
        "country",
        "first_known_browser",
        "web_event_page",
        "first_known_seq_no",
        "seq_no_share",
        "next_seq_no_count_30d",
        "last_known_web_event_duration_sec",
    )
    .orderBy(F.col("next_seq_no_count_30d").desc(), "country")
)
