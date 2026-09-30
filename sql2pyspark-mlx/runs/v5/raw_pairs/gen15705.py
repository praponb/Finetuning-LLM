from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w_running = Window.partitionBy("device_code").orderBy("prev_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("next_pressure", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

with_windows = (
    df_device_readings
    .withColumn("next_pressure", F.lead("pressure").over(w))
    .withColumn("prev_pressure", F.lag("pressure").over(w))
    .withColumn("prev_status_bits", F.lag("status_bits").over(w))
    .withColumn("next_status_bits", F.lead("status_bits").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

enriched = (
    with_windows
    .filter(F.col("row_seq") > 6)
    .select(
        "*",
        F.avg("pressure").over(w_running).alias("running_avg_pressure"),
        F.min("pressure").over(w_running).alias("running_min_pressure"),
        F.count("pressure").over(w_running).alias("running_count_pressure"),
        F.avg("next_pressure").over(w3).alias("next_pressure_avg_2"),
    )
)

matched_keys = (
    enriched
    .select("id", "firmware")
    .intersect(
        df_device_readings
        .filter(F.col("status_bits") >= 8)
        .select("id", "firmware")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

enriched_device_readings = (
    verified
    .withColumn("next_pressure_avg_2_max_1h", F.max("next_pressure_avg_2").over(w4))
)

filtered = (
    enriched_device_readings
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "*",
        (F.col("prev_status_bits") + F.col("status_bits")).alias("prev_status_bits_plus_status_bits"),
    )
)

stats = (
    filtered
    .rollup("device_code")
    .agg(
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.percentile_approx("running_count_pressure", 0.9).alias("running_count_pressure_p90"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
    )
)

result = (
    stats
    .select(
        "device_code",
        "dv_04_rows",
        F.when(F.col("device_code").rlike("^(DV-02|DV-01)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("device_code_zone"),
    )
    .orderBy("device_code_zone")
)
