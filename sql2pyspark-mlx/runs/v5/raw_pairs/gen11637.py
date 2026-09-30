from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("activity_month").orderBy(F.col("span_minutes").desc())

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy(
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .withColumn("span_minutes_dense_rank", F.dense_rank().over(w3))
    .orderBy(F.col("week_start").desc_nulls_last())
)
