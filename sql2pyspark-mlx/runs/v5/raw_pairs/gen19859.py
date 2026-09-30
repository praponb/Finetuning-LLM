from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.hour("reading_ts").between(6, 14))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("pressure") - F.col("temp_c")).alias("pressure_minus_temp_c"),
        (F.col("temp_c") * F.col("pressure")).alias("temp_c_times_pressure"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("temp_c", 0.5).alias("temp_c_p50"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.avg("status_bits").alias("avg_status_bits"),
        F.max("reading_ts").alias("last_seen"),
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
    )
    .orderBy(F.col("activity_day").desc_nulls_last())
)
