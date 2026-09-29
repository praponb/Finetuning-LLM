from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("customer_id").orderBy("event_time", "id")
w4 = Window.partitionBy("customer_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("web_event_event_type").orderBy(F.col("avg_island_id").desc())

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("browser_device_key", F.concat_ws("-", "browser", "device"))
)

with_last = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.duration_sec"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.browser",
        "s.started_at",
        "s.browser_device_key",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.event_time"),
    )
)

flagged = (
    latest_match
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "customer_id",
        "started_at",
        "browser_device_key",
        "web_event_event_type",
        "event_time",
        F.sum("is_new_session").over(w4).alias("island_id"),
    )
)

base = (
    sessions_sessions
    .filter(F.col("web_event_event_type").isNotNull())
    .withColumn("clean_web_event_event_type", F.trim("web_event_event_type"))
    .withColumn("web_event_event_type_clean", F.coalesce("web_event_event_type", F.lit("N/A")))
)

result = (
    base
    .groupBy(
        "web_event_event_type",
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        "customer_id",
    )
    .agg(
        F.size(F.collect_set("web_event_event_type")).alias("num_web_event_event_types"),
        F.count_if(F.col("web_event_event_type") == "add_to_cart").alias("add_to_cart_events"),
        F.sum(F.when(F.col("web_event_event_type") == "add_to_cart", F.col("island_id")).otherwise(0)).alias("add_to_cart_island_id"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.count_if(F.col("web_event_event_type") == "page_view").alias("page_view_events"),
        F.count("*").alias("row_count"),
    )
    .withColumn("avg_island_id_percent_rank", F.percent_rank().over(w5))
    .orderBy(F.col("page_view_events").desc_nulls_last())
)
