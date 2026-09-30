from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

deduped = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "device", "country")
)

with_last = (
    deduped
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.country",
        F.col("c.duration_sec"),
        F.col("c.event_time"),
    )
)

base = (
    with_last
    .filter(F.col("country").isNotNull())
    .select(
        "*",
        F.substring("country", 1, 4).alias("country_prefix"),
        F.coalesce("country", F.lit("DIRECT")).alias("resolved_country"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.duration_sec",
        "s.country_prefix",
        "s.resolved_country",
        F.col("c.event_type"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"), F.col("c.page"))
)

with_last_sessions = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.web_event_event_time",
        "s.page",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.page").alias("web_event_page"),
    )
)

eligible = (
    with_last_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.web_event_event_time")), "left_anti")
)

stats = (
    eligible
    .groupBy("web_event_page")
    .agg(
        F.sum(F.when(F.col("device") == "mobile", F.col("web_event_duration_sec")).otherwise(0)).alias("mobile_web_event_duration_sec"),
        F.percentile_approx("web_event_duration_sec", 0.5).alias("web_event_duration_sec_p50"),
        F.count(F.when(F.col("web_event_page") == "/home", 1)).alias("home_count"),
        F.min("web_event_event_time").alias("started_at"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .filter(F.col("num_devices") > 1)
)

result = (
    stats
    .select("web_event_page", "web_event_duration_sec_p50", "home_count", "num_devices")
)
