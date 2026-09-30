from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2024-05-14 10:00:00").cast("timestamp")) & F.col("page").isNotNull() & (F.to_date("event_time") >= "2024-03-06"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("event_type").alias("event_type_lower"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

filtered = (
    islands
    .filter(F.col("event_time") >= F.to_date(F.lit("2024-02-25")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "event_type_lower",
        "is_new_burst",
        "session_no",
        (F.col("session_no") * F.col("duration_sec")).alias("session_no_times_duration_sec"),
        F.when(F.col("duration_sec") >= 419, "High").when(F.col("duration_sec") >= 206, "Medium").otherwise("Low").alias("duration_sec_level"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

result = (
    filtered
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "event_type_lower",
        "session_no",
        "duration_sec_level",
        F.col("avg_web_event_duration_sec"),
    )
)
