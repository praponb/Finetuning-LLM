from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("pressure_avg_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 08:00:00")))
    .withColumn("pressure_plus_status_bits", (F.col("pressure") + F.col("status_bits")))
)

with_windows = (
    filtered
    .withColumn("pressure_avg_30m", F.avg("pressure").over(w_grp))
    .withColumn("pressure_sum_1h", F.sum("pressure").over(w2))
    .withColumn("temp_c_count_30m", F.count("temp_c").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w3))
)

with_prev = with_windows.withColumn("prev_reading_ts", F.lag("reading_ts").over(w3))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "pressure_avg_30m",
        "pressure_sum_1h",
        "temp_c_count_30m",
        F.sum("new_group_flag").over(w4).alias("island_id"),
    )
)

with_gaps = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands_device_readings = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w4))

metrics = (
    islands_device_readings
    .withColumn("running_pressure_avg_30m", F.sum("pressure_avg_30m").over(w5))
    .withColumn("first_known_temp_c_count_30m", F.first("temp_c_count_30m", ignorenulls=True).over(w6))
    .withColumn("first_known_is_new_burst", F.first("is_new_burst", ignorenulls=True).over(w7))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "status_bits",
        "firmware",
        "pressure_sum_1h",
        "temp_c_count_30m",
        "island_id",
        "is_new_burst",
        "session_id",
        F.when(F.col("device_code").rlike("^(DV-04|DV-01)$"), "CORE").when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "GROUP_A").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("session_id").desc())
)
