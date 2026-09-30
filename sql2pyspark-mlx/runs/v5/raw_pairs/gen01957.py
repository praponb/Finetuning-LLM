from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .groupBy("device_code", F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("humidity"))).alias("dv_04_total_humidity"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("temp_c")).otherwise(0)).alias("dv_04_temp_c"),
        F.avg("session_id").alias("avg_session_id"),
    )
)
