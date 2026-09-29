from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        "device_code",
    )
    .agg(
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .filter(F.col("num_device_codes") >= 1)
    .orderBy(F.col("device_code").desc_nulls_last())
)
