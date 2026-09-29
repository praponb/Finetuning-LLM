from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", F.col("is_new_burst")).otherwise(0)).alias("page_view_is_new_burst"),
        F.min("event_time").alias("started_at"),
    )
    .filter(F.col("page_view_is_new_burst") > 0)
    .orderBy(F.col("page_view_is_new_burst").desc())
)
