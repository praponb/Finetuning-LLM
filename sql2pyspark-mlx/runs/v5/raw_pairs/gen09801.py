from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.device").isin(["tablet", "desktop"]))
    .select("e.*", F.col("p.device"), F.col("p.country"))
)

with_prev = enriched.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "device",
        "country",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

valid = (
    islands
    .filter(F.col("event_time") > F.to_date(F.lit("2023-03-17")))
    .withColumn("event_type_page", F.concat("event_type", F.lit(", "), "page"))
    .withColumn("device_filled", F.coalesce("device", F.lit("N/A")))
)

gaps_2 = (
    valid
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands_web_events = gaps_2.withColumn("session_id", F.sum("is_new_session").over(w4))

result = (
    islands_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("activity_month"))
    .agg(
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("is_new_session_per_page_view"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list_2"),
    )
    .orderBy(F.col("activity_month").desc())
)
