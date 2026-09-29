from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("island_id_tier").orderBy(F.col("num_pages").desc())

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("island_id") >= 8, "LARGE").otherwise("SMALL").alias("island_id_tier"),
        "island_id",
    )
    .agg(
        F.max("event_time").alias("last_seen"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("island_id")).otherwise(0)).alias("page_view_island_id"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("page") == "/home", 1)), F.lit(0)), 2).alias("new_group_flag_per_home"),
        F.max("event_time").alias("last_event_time"),
    )
    .withColumn("num_pages_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("island_id").desc())
)
