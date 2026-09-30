from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("status_bits_whole", F.floor("status_bits"))
)

with_gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .cube("session_id", "hour_window")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("session_id"))).alias("dv_03_total_session_id"),
        F.max(F.when(F.col("device_code") == "DV-04", F.col("status_bits_whole"))).alias("dv_04_max_status_bits_whole"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.avg(F.when(F.col("device_code") == "DV-02", F.col("humidity"))).alias("dv_02_avg_humidity"),
        F.grouping("session_id").alias("grouping_level"),
    )
    .orderBy("dv_04_max_status_bits_whole")
)
