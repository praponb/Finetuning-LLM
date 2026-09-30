from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.hour("reading_ts").between(10, 16))
    .withColumn("pressure_whole", F.ceil("pressure"))
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("humidity") >= 76, "PREMIUM").otherwise("STANDARD").alias("humidity_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
    )
)
