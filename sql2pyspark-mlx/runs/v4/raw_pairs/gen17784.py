from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .cube("activity_day", "device_code", "burst_id")
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list_2"),
        F.grouping("activity_day").alias("grouping_id"),
    )
    .orderBy("activity_day")
)
