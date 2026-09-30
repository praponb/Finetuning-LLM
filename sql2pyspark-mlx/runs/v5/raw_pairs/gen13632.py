from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("page").orderBy("duration_sec", "id")
w4 = Window.orderBy(F.col("duration_sec").desc_nulls_last())
w5 = Window.partitionBy("page").orderBy(F.col("duration_sec_cume_dist").asc_nulls_first(), "id")

filtered = (
    df_web_events
    .filter(~F.col("page").eqNullSafe(F.lit("/search")))
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

cleaned = (
    filtered
    .filter(F.col("days_since_event_time") != 45)
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.least("days_since_event_time", "duration_sec").alias("least_days_since_event_time_duration_sec"),
    )
)

valid = (
    cleaned
    .filter((~F.col("page").eqNullSafe(F.lit("/search"))) & F.col("least_days_since_event_time_duration_sec").between(39, 62))
    .select(
        "id",
        "page",
        "duration_sec",
        F.when(F.col("least_days_since_event_time_duration_sec") >= 70, "High").when(F.col("least_days_since_event_time_duration_sec") >= 11, "Medium").otherwise("Low").alias("least_days_since_event_time_duration_sec_level"),
    )
)

scored = (
    valid
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("duration_sec_bucket", F.ntile(4).over(w3))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w4))
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "page",
        "least_days_since_event_time_duration_sec_level",
        "cumulative_duration_sec",
        "duration_sec_bucket",
        "duration_sec_cume_dist",
    )
)

result = (
    deduped
    .withColumn("cumulative_duration_sec_tier", F.when(F.col("cumulative_duration_sec") >= 1883, "LARGE").otherwise("SMALL"))
    .withColumn("duration_sec_cume_dist_tier", F.when(F.col("duration_sec_cume_dist") >= 0.68, "HIGH").otherwise("LOW"))
    .rollup("cumulative_duration_sec_tier", "duration_sec_cume_dist_tier", "page")
    .agg(
        F.round(F.avg("duration_sec_bucket"), 2).alias("avg_duration_sec_bucket"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.countDistinct("page").alias("distinct_pages"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.grouping("cumulative_duration_sec_tier").alias("grouping_id"),
    )
    .filter(F.col("avg_duration_sec_bucket") < 1000000)
    .orderBy(F.col("cart_count").desc_nulls_first())
)
