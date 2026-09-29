from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-09-25 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "page_event_type_key",
        "resolved_page",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        F.date_trunc("week", "event_time").alias("week_start"),
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
    )
    .agg(
        F.min("event_time").alias("first_seen"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.min("event_time").alias("first_event_time"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
    .filter(F.col("distinct_session_ids") > 1)
)
