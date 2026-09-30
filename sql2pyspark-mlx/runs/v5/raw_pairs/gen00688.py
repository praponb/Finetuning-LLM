from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "is_new_burst",
        F.when(F.col("event_type").rlike("^(add_to_cart|page_view)$"), "OTHER_ZONE").when(F.col("event_type").rlike("^(click|purchase)$"), "GROUP_A").otherwise("PRIORITY").alias("event_type_zone"),
    )
    .orderBy("id", "is_new_burst")
)
