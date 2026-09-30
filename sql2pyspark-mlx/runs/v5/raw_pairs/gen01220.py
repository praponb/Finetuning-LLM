from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .rollup(
        F.when(F.col("duration_sec") >= 374, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.min("event_time").alias("started_at"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
    .filter(F.col("search_count") >= 1)
    .orderBy(F.col("duration_sec_tier").desc())
)
