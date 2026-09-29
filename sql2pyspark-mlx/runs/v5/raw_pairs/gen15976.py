from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("firmware").isin(["2.0.1", "1.1.0", "1.0.3"]))
    .select("*", (F.col("temp_c") - F.col("humidity")).alias("temp_c_minus_humidity"))
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.corr("temp_c", "status_bits").alias("corr_temp_c_status_bits"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("temp_c_minus_humidity")).otherwise(0)).alias("dv_04_temp_c_minus_humidity"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
    )
    .orderBy(F.col("hour_window").desc_nulls_first())
)
