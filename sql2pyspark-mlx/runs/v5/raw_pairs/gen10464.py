from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)

metrics = (
    df_web_events
    .select(
        "*",
        F.ntile(5).over(w).alias("duration_sec_bucket"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec_2"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_5"),
    )
)

result = (
    metrics
    .withColumn("cumulative_duration_sec_tier", F.when(F.col("cumulative_duration_sec") >= 2378, "LARGE").otherwise("SMALL"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 465, "PREMIUM").otherwise("STANDARD"))
    .cube("cumulative_duration_sec_tier", "duration_sec_tier")
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.sum("duration_sec_bucket").alias("total_duration_sec_bucket"),
        F.grouping("cumulative_duration_sec_tier").alias("gid"),
    )
    .orderBy("duration_sec_tier")
)
