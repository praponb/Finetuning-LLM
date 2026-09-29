from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-08-21").cast("date")) & F.col("page").isNotNull())
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["click", "page_view"]), "Group A").otherwise("Other"))
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 406, "High").when(F.col("duration_sec") >= 206, "Medium").otherwise("Low"))
)

gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.avg(F.when(F.col("event_type") == "search", F.col("is_new_session"))).alias("search_avg_is_new_session"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.avg(F.when(F.col("event_type") == "page_view", F.col("island_id"))).alias("page_view_avg_island_id"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("num_event_types") > 1)
)
