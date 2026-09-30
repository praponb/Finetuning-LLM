from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())
w6 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.orderBy("event_time", "id")

cleaned = (
    df_web_events
    .filter(F.hour("event_time").between(11, 16) & (F.col("event_time") > F.lit("2023-04-19").cast("date")))
    .select("*", F.concat_ws("/", "event_type", "page").alias("event_type_page_key"))
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "session_id", "event_time", "duration_sec")
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

cleaned_web_events = (
    deduped
    .filter((F.col("event_time") >= F.lit("2023-10-21 23:00:00").cast("timestamp")) & F.col("session_id").isin([3, 10, 12, 38]))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 1))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)

with_gaps = (
    cleaned_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec_filled",
        F.sum("is_new_burst").over(w4).alias("session_id_2"),
    )
)

latest_web_events = (
    islands
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest_web_events
    .select(
        "*",
        F.last("duration_sec_filled", ignorenulls=True).over(w6).alias("last_known_duration_sec_filled"),
        F.avg("duration_sec_filled").over(w7).alias("duration_sec_filled_avg_90d"),
        F.row_number().over(w8).alias("seq_no"),
    )
)

result = (
    enriched
    .select(
        "id",
        "session_id",
        "duration_sec_filled",
        "session_id_2",
        "last_known_duration_sec_filled",
        "duration_sec_filled_avg_90d",
        "seq_no",
    )
    .orderBy(F.col("duration_sec_filled").desc(), "session_id")
)
