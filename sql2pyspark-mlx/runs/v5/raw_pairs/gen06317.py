from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("event_time").desc(), "id")
w2 = Window.partitionBy("device", "page").orderBy(F.col("event_time").desc_nulls_last())
w3 = Window.orderBy("days_since_event_time_minus_event_time_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("event_time_hour").desc_nulls_last())
w5 = Window.orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.event_time",
        F.col("p.country"),
        F.col("p.device"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "page", "event_time", "country", "device")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "event_time")
)

cleaned = (
    ranked
    .filter(F.col("event_time") > F.lit("2023-05-07 09:00:00").cast("timestamp"))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .withColumn("event_time_day", F.col("event_time").cast("date"))
)

filtered = (
    cleaned
    .filter(F.col("event_time") >= F.lit("2023-04-22 08:00:00").cast("timestamp"))
    .select("*", F.hour("event_time").alias("event_time_hour"))
)

filtered_web_events = (
    filtered
    .filter(F.col("event_time") >= F.lit("2023-04-28 01:00:00").cast("timestamp"))
    .select(
        "id",
        "event_time",
        "event_time_day",
        "event_time_hour",
        (F.col("days_since_event_time") - F.col("event_time_hour")).alias("days_since_event_time_minus_event_time_hour"),
    )
)

enriched_web_events = (
    filtered_web_events
    .select(
        "id",
        "event_time_hour",
        F.last("event_time_hour", ignorenulls=True).over(w3).alias("last_known_event_time_hour"),
        F.dense_rank().over(w4).alias("event_time_hour_dense_rank"),
        F.last("event_time_hour", ignorenulls=True).over(w5).alias("last_known_event_time_hour_2"),
        F.sum("event_time_hour").over(w6).alias("running_event_time_hour"),
    )
)

result = (
    enriched_web_events
    .groupBy(
        F.when(F.col("running_event_time_hour") >= 360, "LARGE").otherwise("SMALL").alias("running_event_time_hour_tier"),
        F.when(F.col("last_known_event_time_hour_2") >= 71, "LARGE").otherwise("SMALL").alias("last_known_event_time_hour_2_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("last_known_event_time_hour_2"), 2).alias("avg_last_known_event_time_hour_2"),
        F.sum("event_time_hour_dense_rank").alias("total_event_time_hour_dense_rank"),
        F.max("last_known_event_time_hour").alias("max_last_known_event_time_hour"),
    )
    .filter(F.col("avg_last_known_event_time_hour_2") < 1000000)
)
