from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_device_readings
    .groupBy("island_id", "device_code")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.max(F.when(F.col("device_code") == "DV-02", F.col("is_new_session"))).alias("dv_02_max_is_new_session"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.avg(F.when(F.col("device_code") == "DV-03", F.col("temp_c"))).alias("dv_03_avg_temp_c"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("pressure")).otherwise(0)).alias("dv_03_pressure"),
    )
    .orderBy(F.col("dv_04_count").asc_nulls_last())
)
