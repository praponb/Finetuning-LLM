from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.percentile_approx("temp_c", 0.5).alias("temp_c_p50"),
    )
)
