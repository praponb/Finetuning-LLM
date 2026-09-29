from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("session_id_2"),
    )
)

result = (
    sessions_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        F.when(F.col("event_type").rlike("^(purchase|add_to_cart)$"), "PRIORITY").otherwise("CORE").alias("event_type_zone"),
    )
    .orderBy(F.col("event_type_zone").desc())
)
