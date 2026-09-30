from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("event_time_hour", "id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("session_id") == 24))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

result = (
    valid
    .withColumn("last_known_event_time_hour", F.last("event_time_hour", ignorenulls=True).over(w))
    .withColumn("event_time_hour_bucket", F.ntile(5).over(w2))
)
