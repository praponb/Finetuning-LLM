from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .select(
        "*",
        F.sum("pressure").over(w).alias("running_pressure"),
        F.lead("status_bits", 2).over(w2).alias("next_status_bits"),
        F.last("pressure", ignorenulls=True).over(w3).alias("last_known_pressure"),
        F.sum("temp_c").over(w3).alias("running_temp_c"),
    )
)

result = (
    enriched
    .cube(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.count("*").alias("num_rows"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("running_pressure")).otherwise(0)).alias("dv_02_running_pressure"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.percentile_approx("running_pressure", [0.25, 0.5, 0.75]).alias("running_pressure_quartiles"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
