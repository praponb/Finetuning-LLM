from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .select("id", "island_id")
    .orderBy(F.col("id").desc_nulls_first())
)
