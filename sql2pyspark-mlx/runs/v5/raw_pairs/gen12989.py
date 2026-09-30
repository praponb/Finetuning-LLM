from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("temp_c", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, -1)
w_hist = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")
w8 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_device_readings
    .filter(F.col("reading_ts") < "2024-08-01")
    .select(
        "*",
        F.last("temp_c", ignorenulls=True).over(w).alias("last_known_temp_c"),
        F.lag("temp_c", 2).over(w2).alias("prev_temp_c"),
        F.avg("status_bits").over(w3).alias("status_bits_avg_2_prior"),
        F.lead("humidity", 2).over(w_hist).alias("next_humidity"),
        F.sum("pressure").over(w5).alias("pressure_sum_15m"),
    )
)

with_prev = metrics.withColumn("prev_reading_ts", F.lag("reading_ts").over(w_hist))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w6))

gaps_2 = (
    islands
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w_hist) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = (
    gaps_2
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "status_bits_avg_2_prior",
        "next_humidity",
        "prev_reading_ts",
        F.sum("is_new_burst").over(w6).alias("burst_id"),
    )
)

with_gaps = (
    sessionized
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts") - F.lag("prev_reading_ts").over(w7) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "temp_c",
        "next_humidity",
        "prev_reading_ts",
        F.sum("session_start_flag").over(w8).alias("session_id"),
    )
)

result = (
    islands_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "next_humidity",
        "session_id",
        F.when(F.col("device_code").rlike("^(DV-02|DV-01)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("session_id").desc_nulls_last(), "id")
    .limit(10)
)
