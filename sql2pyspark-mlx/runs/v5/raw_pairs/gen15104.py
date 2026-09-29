from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-07-03 23:00:00"))) & F.hour("event_time").between(6, 10))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.concat_ws("/", "page", "event_type").alias("page_event_type_key"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("duration_sec") >= 446, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.max("event_time").alias("last_seen"),
        F.min("event_time").alias("first_event_time"),
        F.count("*").alias("cnt"),
    )
)
