from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 09:00:00").cast("timestamp"))
    .withColumn("status_bits_times_humidity", (F.col("status_bits") * F.col("humidity")))
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 34.0, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("session_start_flag")).otherwise(0)).alias("dv_02_session_start_flag"),
        F.min("reading_ts").alias("first_seen"),
    )
)
