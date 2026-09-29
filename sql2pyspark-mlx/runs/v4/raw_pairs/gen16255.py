from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("reading_ts", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("reading_ts", "id")

cleaned = (
    df_device_readings
    .filter(F.col("humidity") > 43)
    .select(
        "*",
        F.round("pressure", 1).alias("pressure_rounded"),
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    deduped
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 11:00:00")))
    .withColumn("firmware_upper", F.upper("firmware"))
)

scored = (
    filtered
    .withColumn("cumulative_pressure_rounded", F.sum("pressure_rounded").over(w2))
    .withColumn("temp_c_avg_6_prior", F.avg("temp_c").over(w3))
    .withColumn("humidity_avg_3", F.avg("humidity").over(w4))
    .withColumn("pressure_rounded_max_30m", F.max("pressure_rounded").over(w_running))
    .withColumn("status_bits_avg_15m", F.avg("status_bits").over(w6))
)

flagged = (
    scored
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w7) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

kept_keys = (
    sessions_device_readings
    .select("device_code", "firmware")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") < 7)
        .select("device_code", "firmware")
    )
)

reconciled = (
    sessions_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("firmware")
    .pivot("device_code", ["DV-04", "DV-02", "DV-03"])
    .agg(F.sum("humidity_avg_3"))
    .select(
        "firmware",
        F.col("DV-04").alias("dv_04"),
        F.col("DV-02").alias("dv_02"),
        F.col("DV-03").alias("dv_03"),
    )
)
