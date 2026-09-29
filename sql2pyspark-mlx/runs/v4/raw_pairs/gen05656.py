from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .select("id", "reading_ts", "humidity", "status_bits", "new_group_flag", "burst_id")
    .orderBy(F.col("new_group_flag").asc_nulls_first())
)
