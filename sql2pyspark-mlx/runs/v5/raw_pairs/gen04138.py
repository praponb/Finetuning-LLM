from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

kept_keys = (
    sessions_web_events
    .select("event_type", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") >= 258)
        .select("event_type", "session_id")
    )
)

kept_rows = (
    sessions_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .filter(~F.col("p2.browser").isin(["Safari", "Chrome", "Edge"]))
    .select(
        "s.session_id",
        "s.event_type",
        "s.event_time",
        F.col("p2.country"),
        F.col("p2.device"),
        F.col("p2.browser"),
    )
)

filtered = (
    with_dims
    .filter(F.col("event_type") == "click")
    .select(
        "session_id",
        "event_type",
        "event_time",
        "country",
        "device",
        "browser",
        F.concat("country", F.lit(" "), "device").alias("country_device"),
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
)

result = (
    filtered
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "event_type",
        "event_time",
        "country",
        "device",
        "browser",
        "country_device",
        "event_type_title",
        F.col("sum_web_event_duration_sec"),
    )
)
