from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("device_code_avg_pressure_filled", "id")
w6 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.orderBy("session_id", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy(F.col("device_code_sum_session_id").desc_nulls_last())

valid = (
    df_device_readings
    .filter(F.col("temp_c").isNotNull())
    .withColumn("pressure_filled", F.coalesce("pressure", F.lit(0)))
    .withColumn("firmware_prefix", F.substring("firmware", 1, 3))
)

with_gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

enriched = (
    islands
    .select(
        "*",
        F.last("new_group_flag", ignorenulls=True).over(w3).alias("last_known_new_group_flag"),
        F.sum("session_id").over(w_ordered).alias("device_code_sum_session_id"),
        F.avg("pressure_filled").over(w_ordered).alias("device_code_avg_pressure_filled"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_device_readings = (
    latest
    .select(
        "*",
        F.sum("new_group_flag").over(w6).alias("new_group_flag_sum_30m"),
        F.avg("status_bits").over(w7).alias("status_bits_avg_5"),
        F.corr("humidity", "status_bits").over(w_ordered).alias("corr_humidity_status_bits"),
    )
)

with_windows = (
    enriched_device_readings
    .withColumn("device_code_stddev_samp_device_code_avg_pressure_filled", F.stddev_samp("device_code_avg_pressure_filled").over(w_ordered))
    .withColumn("device_code_sum_session_id_percent_rank", F.percent_rank().over(w8))
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "pressure_filled",
        "firmware_prefix",
        "new_group_flag",
        F.when(F.col("device_code").rlike("^(DV-03|DV-01)$"), "GROUP_A").when(F.col("device_code").rlike("^(DV-02|DV-04)$"), "CORE").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("pressure_filled").desc())
)
