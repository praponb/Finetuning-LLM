from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

filtered = (
    valid
    .filter((F.col("event_time") > F.to_date(F.lit("2023-10-11"))) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "event_time_hour",
        "page_clean",
        F.col("event_time_hour").cast("double").alias("event_time_hour_num"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean_2"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        "event_time_hour",
        "page_clean",
        "event_time_hour_num",
        "page_clean_2",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

flagged_2 = (
    sessions_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged_2.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

grouped = (
    islands
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
        "session_no",
    )
    .agg(
        F.percentile_approx("event_time_hour_num", [0.25, 0.5, 0.75]).alias("event_time_hour_num_quartiles"),
        F.max("event_time").alias("last_event_time"),
        F.max("event_time").alias("last_event_time_2"),
        F.max("event_time").alias("last_seen"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
)

result = (
    grouped
    .select(
        "is_new_burst_tier",
        "session_no",
        "event_time_hour_num_quartiles",
        "last_event_time",
        "last_seen",
    )
)
