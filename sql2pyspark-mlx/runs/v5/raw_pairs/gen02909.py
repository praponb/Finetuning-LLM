from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("customer_id").orderBy("web_event_duration_sec", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-03-10 22:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.substring("browser", 1, 4).alias("browser_prefix"),
        F.trim("device").alias("clean_device"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

enriched = (
    with_latest
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "browser_prefix",
        "country_filled",
        "web_event_page",
        F.last("web_event_duration_sec", ignorenulls=True).over(w2).alias("last_known_web_event_duration_sec"),
        F.avg("web_event_duration_sec").over(w_running).alias("web_event_duration_sec_avg_2"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .cube("activity_month")
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.min("started_at").alias("first_seen"),
        F.min("last_known_web_event_duration_sec").alias("min_last_known_web_event_duration_sec"),
        F.sort_array(F.collect_set("web_event_page")).alias("web_event_page_list"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .orderBy("activity_month")
)
