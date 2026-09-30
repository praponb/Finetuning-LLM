from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-08-13").cast("date")) & (F.col("page").eqNullSafe(F.lit("/search"))))
    .select(
        "*",
        F.hour("event_time").alias("event_time_hour"),
        F.when(F.col("event_type").isin(["click", "search"]), "Core").otherwise("Other").alias("event_type_group"),
    )
)

valid = (
    cleaned
    .filter(F.col("event_time_hour") <= 33)
    .withColumn("event_time_hour_str", F.col("event_time_hour").cast("double"))
    .withColumn("duration_sec_num", F.col("duration_sec").cast("string"))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "event_time_hour",
        "event_time_hour_str",
        "duration_sec_num",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

base = (
    sessions_web_events
    .filter(F.col("event_time") > F.lit("2023-05-23 05:00:00").cast("timestamp"))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "event_time_hour",
        "event_time_hour_str",
        "duration_sec_num",
        "session_no",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)

result = (
    base
    .select(
        "id",
        "page",
        "event_time",
        "event_time_hour_str",
        "duration_sec_num",
        "session_no",
        "event_time_label",
    )
)
