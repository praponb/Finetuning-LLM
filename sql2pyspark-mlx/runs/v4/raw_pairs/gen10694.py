from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("island_id") >= 8, "HIGH").otherwise("LOW").alias("island_id_tier"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.count_if(F.col("page") == "/home").alias("home_events"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("event_type") == "search", 1)), F.lit(0)), 2).alias("island_id_per_search"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
    .filter(F.col("island_id_per_search") > 0)
    .orderBy(F.col("island_id_tier").desc_nulls_first())
)
