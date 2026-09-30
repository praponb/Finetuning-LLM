from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy("new_group_flag", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("page").orderBy("event_time", "id")

flagged = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

scored = (
    sessionized
    .withColumn("duration_sec_avg_3", F.avg("duration_sec").over(w3))
    .withColumn("running_new_group_flag", F.sum("new_group_flag").over(w4))
)

flagged_2 = (
    scored
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w5).isNull() | (F.col("event_time") > F.lag("event_time").over(w5) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged_2.withColumn("session_id_3", F.sum("session_start_flag").over(w4))

result = (
    sessions_web_events
    .groupBy("session_id")
    .pivot("page", ["/search", "/cart"])
    .agg(F.max("duration_sec"))
    .select(
        "session_id",
        F.col("/search").alias("search"),
        F.col("/cart").alias("cart"),
    )
)
