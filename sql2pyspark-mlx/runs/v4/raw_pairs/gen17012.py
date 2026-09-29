from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("status_bits") >= 12, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        "device_code",
    )
    .agg(
        F.min("reading_ts").alias("first_reading_ts"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("pressure_per_dv_04"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
    )
    .orderBy(F.col("device_code").desc_nulls_last())
)
