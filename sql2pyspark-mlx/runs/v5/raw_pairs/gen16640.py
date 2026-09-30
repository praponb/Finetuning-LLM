from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id", "event_type").orderBy(F.col("event_time_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("last_known_duration_sec").asc_nulls_first())
w4 = Window.partitionBy("event_type").orderBy("event_time", "id")
w5 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-08-13 19:00:00"))) & F.col("page").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("page")).alias("page_title"),
        F.date_trunc("month", "event_time").alias("event_time_period"),
    )
)

enriched = (
    valid
    .withColumn("last_known_duration_sec", F.last("duration_sec", ignorenulls=True).over(w))
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w2))
)

ranked = (
    enriched
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

with_gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w5))

result = (
    sessions_web_events
    .withColumn("session_id_2_tier", F.when(F.col("session_id_2") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .rollup("session_id_2_tier", "year_start")
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.percentile_approx("last_known_duration_sec", 0.95).alias("last_known_duration_sec_p95"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.grouping("session_id_2_tier").alias("grouping_level"),
    )
    .orderBy(F.col("distinct_event_types").desc_nulls_last())
)
