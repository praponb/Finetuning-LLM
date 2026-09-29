from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("is_new_burst_tier").orderBy(F.col("span_minutes").desc())

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
        F.sum("is_new_burst").alias("total_is_new_burst"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("cnt"),
    )
    .withColumn("span_minutes_rank", F.rank().over(w3))
)
