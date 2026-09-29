from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 10:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        (F.col("pressure") * F.col("temp_c")).alias("pressure_times_temp_c"),
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

prev_events = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("pressure_times_temp_c") >= 81, "PREMIUM").otherwise("STANDARD").alias("pressure_times_temp_c_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
    )
    .orderBy(F.col("pressure_times_temp_c_tier").desc())
)
