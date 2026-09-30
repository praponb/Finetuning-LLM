from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp")) & F.hour("reading_ts").between(8, 16))
    .withColumn("status_bits_minus_pressure", (F.col("status_bits") - F.col("pressure")))
)

with_gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1029, "LARGE").otherwise("SMALL"))
    .rollup("pressure_tier", "session_id", "device_code")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.corr("humidity", "temp_c").alias("corr_humidity_temp_c"),
        F.grouping("pressure_tier").alias("grouping_id"),
    )
    .orderBy("corr_humidity_temp_c")
)
