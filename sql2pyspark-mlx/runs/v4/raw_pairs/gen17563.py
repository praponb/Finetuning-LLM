from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").asc_nulls_first(), "id")
w2 = Window.orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").asc_nulls_last())
w4 = Window.partitionBy("session_id", "event_type")
w5 = Window.partitionBy("event_type").orderBy("event_time", "id")
w6 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.hour("event_time").alias("event_time_hour"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.rank().over(w3).alias("duration_sec_rank"),
        F.avg("event_time_hour").over(w4).alias("session_id_avg_event_time_hour"),
    )
)

flagged = (
    enriched
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w5).isNull() | (F.col("event_time") > F.lag("event_time").over(w5) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w6))

base = (
    islands
    .filter((F.col("event_time") >= F.lit("2023-12-13 08:00:00").cast("timestamp")) & (F.col("event_type") != "search"))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "latest_rank",
        "seq_no",
        "duration_sec_rank",
        "burst_id",
        F.when(F.col("event_type").isin(["add_to_cart", "click"]), "Core").otherwise("Other").alias("event_type_group"),
    )
)

result = (
    base
    .cube(
        F.date_trunc("week", "event_time").alias("week_start"),
        F.date_trunc("month", "event_time").alias("activity_month"),
    )
    .agg(
        F.min("event_time").alias("started_at"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") < 0)
)
