from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        "device_code",
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
    )
    .agg(
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("session_no")).otherwise(0)).alias("dv_02_session_no"),
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
    )
    .filter(F.col("avg_status_bits") > 0)
)
