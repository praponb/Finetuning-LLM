from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("day", "reading_ts").alias("activity_day"), "session_id")
    .agg(
        F.max("status_bits").alias("max_status_bits"),
        F.count("*").alias("num_rows"),
        F.avg("pressure").alias("avg_pressure"),
    )
    .orderBy(F.col("activity_day").desc_nulls_last())
)
