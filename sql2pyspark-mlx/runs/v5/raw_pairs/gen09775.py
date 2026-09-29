from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= F.lit("2023-10-21").cast("date")))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.date_trunc("WEEK", "event_time").alias("event_time_period"),
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.device") == "mobile")
    .select("e.*", F.col("d.device"), F.col("d.browser"), F.col("d.country"))
)

kept_keys = (
    joined
    .select("session_id")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([6, 10, 12, 14]))
        .select("session_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("session_id_sum_duration_sec_num", F.sum("duration_sec_num").over(w))
)

result = (
    with_windows
    .alias("a")
    .join(df_sessions.alias("dim"), F.col("a.session_id") == F.col("dim.id"), "left")
    .rollup(
        F.col("dim.browser").alias("session_browser"),
        F.col("dim.country").alias("session_country"),
        F.col("a.country").alias("web_event_country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.session_id_sum_duration_sec_num")), 2).alias("avg_session_id_sum_duration_sec_num"),
        F.max(F.col("a.duration_sec_num")).alias("max_duration_sec_num"),
    )
    .orderBy(F.col("web_event_country").desc_nulls_first())
)
