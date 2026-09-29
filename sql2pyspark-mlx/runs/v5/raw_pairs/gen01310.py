from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        F.when(F.col("firmware") == "1.0.3", 1).otherwise(0).alias("is_1_0_3"),
        F.when(F.col("temp_c") >= 38, "High").when(F.col("temp_c") >= 24.2, "Medium").otherwise("Low").alias("temp_c_tier"),
    )
)

flagged = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("pressure") >= 1029, "LARGE").otherwise("SMALL").alias("pressure_tier"),
    )
    .agg(
        F.avg("humidity").alias("avg_humidity"),
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("status_bits_per_dv_04"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("temp_c")).otherwise(0)).alias("dv_04_temp_c"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("avg_humidity") < 1000000)
)
