from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("pressure").between(1024, 1027))
    .withColumn("firmware_device_code", F.concat("firmware", F.lit(" "), "device_code"))
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy("device_code")
    .agg(
        F.percentile_approx("temp_c", 0.9).alias("temp_c_p90"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.sum("temp_c").alias("total_temp_c"),
    )
    .orderBy(F.col("dv_04_rows").desc())
)
