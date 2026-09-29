from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list_2"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.avg("session_no").alias("avg_session_no"),
    )
)
