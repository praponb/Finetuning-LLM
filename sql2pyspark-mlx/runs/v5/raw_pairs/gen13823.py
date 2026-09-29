from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .select("id", "event_type", "page", "event_time", "session_no")
    .orderBy(F.col("event_time").desc())
)
