from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-04-09 06:00:00"))) & (F.col("duration_sec") == 360))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.trim("page").alias("clean_page"),
        F.concat("page", F.lit("/"), "event_type").alias("page_event_type"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

flagged = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .rollup("island_id", "event_type")
    .agg(
        F.min("event_time").alias("first_seen"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
    )
    .orderBy(F.col("checkout_rows").asc_nulls_first())
)
