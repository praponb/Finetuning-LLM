from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc())
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("temp_c", "id")
w4 = Window.partitionBy("device_code")

with_windows = (
    df_device_readings
    .select(
        "*",
        F.cume_dist().over(w).alias("humidity_cume_dist"),
        F.sum("pressure").over(w2).alias("cumulative_pressure"),
        F.row_number().over(w_running).alias("event_seq"),
        F.min("humidity").over(w4).alias("device_code_min_humidity"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("humidity_cume_dist") >= 0.74, "HIGH").otherwise("LOW").alias("humidity_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_pressure", 0.5).alias("cumulative_pressure_p50"),
        F.max("pressure").alias("max_pressure"),
        F.max("reading_ts").alias("last_seen"),
    )
    .filter(F.col("cumulative_pressure_p50") < 0)
    .orderBy("max_pressure")
)
