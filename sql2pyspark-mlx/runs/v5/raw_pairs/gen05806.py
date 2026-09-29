from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.orderBy("duration_sec", "id")
w_running = Window.partitionBy("session_browser").orderBy("next_duration_sec", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_web_events
    .withColumn("prev_duration_sec", F.lag("duration_sec", 1).over(w))
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w2))
)

with_dims = (
    scored
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.device") != "desktop")
    .select(
        "s.id",
        "s.session_id",
        "s.event_time",
        "s.prev_duration_sec",
        "s.next_duration_sec",
        F.col("b.country"),
        F.col("b.browser"),
        F.col("b.device"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.country").alias("session_country"))
)

joined_web_events = (
    joined
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.prev_duration_sec",
        "e.next_duration_sec",
        "e.country",
        "e.browser",
        F.col("d.country").alias("session_country_2"),
        F.col("d.browser").alias("session_browser"),
        F.col("d.device").alias("session_device"),
    )
)

enriched = (
    joined_web_events
    .select(
        "id",
        "prev_duration_sec",
        "next_duration_sec",
        "browser",
        "session_browser",
        F.max("prev_duration_sec").over(w_running).alias("last4_max_prev_duration_sec"),
        F.min("prev_duration_sec").over(w_running).alias("last4_min_prev_duration_sec"),
        F.avg("prev_duration_sec").over(w_running).alias("last4_avg_prev_duration_sec"),
        F.sum("prev_duration_sec").over(w_running).alias("last4_sum_prev_duration_sec"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .exceptAll(
        df_web_events
        .filter(F.col("duration_sec").between(240, 340))
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("session_browser")
    .agg(
        F.percentile_approx("last4_avg_prev_duration_sec", [0.25, 0.5, 0.75]).alias("last4_avg_prev_duration_sec_quartiles"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("last4_avg_prev_duration_sec")).otherwise(0)).alias("chrome_last4_avg_prev_duration_sec"),
        F.sum(F.when(F.col("browser") == "Firefox", F.col("prev_duration_sec")).otherwise(0)).alias("firefox_prev_duration_sec"),
    )
)
