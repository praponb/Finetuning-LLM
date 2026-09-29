from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "is_new_burst",
        "session_no",
        F.when(F.col("event_type").rlike("^(search|purchase)$"), "GROUP_A").otherwise("CORE").alias("event_type_zone"),
    )
    .orderBy(F.col("event_type").desc(), "event_time")
)
