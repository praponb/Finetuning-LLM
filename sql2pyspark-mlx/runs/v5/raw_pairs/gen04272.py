from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("event_time").desc(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("session_id").isin([34, 40]))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.when(F.col("duration_sec") >= 435, "High").when(F.col("duration_sec") >= 223, "Medium").otherwise("Low").alias("duration_sec_level"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        F.sum("session_start_flag").over(w3).alias("burst_id"),
    )
)

base_web_events = (
    islands
    .filter(F.col("page").isNotNull() & (F.col("event_time") > F.lit("2023-03-11 09:00:00").cast("timestamp")))
    .withColumn("event_time_day", F.col("event_time").cast("date"))
    .withColumn("event_time_hour", F.hour("event_time"))
)

result = (
    base_web_events
    .select("id", "page", "event_time", "burst_id", "event_time_day", "event_time_hour")
    .orderBy(F.col("event_time_hour").asc_nulls_last())
)
