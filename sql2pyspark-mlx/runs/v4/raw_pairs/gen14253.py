from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_device_readings
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("running_status_bits", F.sum("status_bits").over(w2))
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w3))
)

result = (
    with_windows
    .groupBy("device_code")
    .agg(
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.min("reading_ts").alias("started_at"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.percentile_approx("pressure", 0.95).alias("pressure_p95"),
    )
    .orderBy(F.col("dv_01_events").asc_nulls_first())
)
