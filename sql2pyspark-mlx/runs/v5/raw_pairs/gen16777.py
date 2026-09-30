from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 01:00:00").cast("timestamp"))
    .withColumn("least_pressure_temp_c", F.least("pressure", "temp_c"))
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .rollup("device_code", "activity_day", "session_id")
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.percentile_approx("temp_c", 0.5).alias("temp_c_p50"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.grouping("device_code").alias("grouping_id"),
    )
    .filter(F.col("dv_02_rows") > 1)
    .orderBy(F.col("temp_c_p50").desc_nulls_last())
)
