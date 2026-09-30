from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-3, -1)
w_part = Window.partitionBy("device_code").orderBy("pressure_count_15m", "id")
w8 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w11 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.to_date("reading_ts") < "2025-09-07")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("status_bits") >= 13, "High").when(F.col("status_bits") >= 4, "Medium").otherwise("Low").alias("status_bits_tier"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("pressure_percent_rank"),
        F.count("pressure").over(w2).alias("pressure_count_15m"),
        F.count("pressure").over(w3).alias("device_code_count_pressure"),
        F.first("temp_c", ignorenulls=True).over(w4).alias("first_known_temp_c"),
    )
)

scored = (
    with_windows
    .withColumn("first_known_temp_c_avg_3", F.avg("first_known_temp_c").over(w5))
    .withColumn("pressure_percent_rank_avg_3_prior", F.avg("pressure_percent_rank").over(w6))
    .withColumn("row_seq", F.row_number().over(w_part))
    .withColumn("last_known_pressure_percent_rank", F.last("pressure_percent_rank", ignorenulls=True).over(w8))
    .withColumn("running_pressure", F.sum("pressure").over(w9))
)

filtered = (
    scored
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("device_code_count_pressure").between(5, 13))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "status_bits_tier",
        "pressure_percent_rank",
        "pressure_count_15m",
        "device_code_count_pressure",
        "first_known_temp_c",
        "first_known_temp_c_avg_3",
        "pressure_percent_rank_avg_3_prior",
        "last_known_pressure_percent_rank",
        "running_pressure",
        F.date_format("reading_ts", "yyyy-MM").alias("reading_ts_label"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w10).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w10) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_id", F.sum("new_group_flag").over(w11))

gaps = (
    sessions_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w10) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w11))

filtered_device_readings = (
    islands
    .filter(F.col("pressure") <= 1001)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "status_bits_tier",
        "pressure_percent_rank",
        "pressure_count_15m",
        "device_code_count_pressure",
        "first_known_temp_c_avg_3",
        "last_known_pressure_percent_rank",
        "running_pressure",
        "reading_ts_label",
        "new_group_flag",
        "session_id",
        "is_new_burst",
        F.least("device_code_count_pressure", "session_id").alias("least_device_code_count_pressure_session_id"),
    )
)

daily = (
    filtered_device_readings
    .groupBy("device_code")
    .agg(
        F.min("reading_ts").alias("first_reading_ts"),
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
)

result = (
    daily
    .select(
        "device_code",
        "first_reading_ts",
        "corr_pressure_status_bits",
        "span_minutes",
    )
    .orderBy(F.col("first_reading_ts").desc())
)
