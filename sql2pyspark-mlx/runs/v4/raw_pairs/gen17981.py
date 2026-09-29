from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy("session_no")
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.min("reading_ts").alias("first_reading_ts"),
    )
    .orderBy(F.col("pressure_stddev").desc())
)
