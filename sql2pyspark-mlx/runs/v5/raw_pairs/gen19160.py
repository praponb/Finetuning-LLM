from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("duration_sec_tier").orderBy(F.col("is_new_burst_p90").desc())

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("duration_sec") >= 533, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        F.when(F.col("session_id_2") >= 9, "LARGE").otherwise("SMALL").alias("session_id_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.count_if(F.col("page") == "/home").alias("home_events"),
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
    )
    .withColumn("is_new_burst_p90_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("duration_sec_tier").desc_nulls_last())
)
