from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("seq_no")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("browser", "page").orderBy(F.col("running_sum_seq_no").asc_nulls_last())

enriched = (
    df_sessions
    .select(
        "id",
        "device",
        "browser",
        "country",
        F.row_number().over(w).alias("seq_no"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "browser",
        "seq_no",
        F.sum("seq_no").over(w2).alias("running_sum_seq_no"),
        F.min("seq_no").over(w2).alias("running_min_seq_no"),
        F.avg("seq_no").over(w2).alias("running_avg_seq_no"),
        F.max("seq_no").over(w2).alias("running_max_seq_no"),
        F.cume_dist().over(w3).alias("seq_no_cume_dist"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_type") != "page_view"), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.seq_no",
        "s.running_sum_seq_no",
        "s.running_avg_seq_no",
        F.col("c.page"),
    )
)

eligible_sessions = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page") == "/product"), "left_semi")
)

with_last = (
    eligible_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.running_sum_seq_no",
        "s.page",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

latest = (
    with_last
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "browser", "web_event_event_time")
)

result = (
    latest
    .groupBy(
        "browser",
        F.date_trunc("YEAR", "web_event_event_time").alias("year_start"),
    )
    .agg(
        F.min("web_event_event_time").alias("first_web_event_event_time"),
        ((F.unix_timestamp(F.max("web_event_event_time")) - F.unix_timestamp(F.min("web_event_event_time"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
    )
    .orderBy(F.col("browser").desc())
)
