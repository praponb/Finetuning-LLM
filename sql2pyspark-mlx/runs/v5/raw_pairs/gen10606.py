from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("status_bits_avg_30m_filled").desc(), "id")
w5 = Window.orderBy("status_bits_avg_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

with_windows = (
    sessionized
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "*",
        F.avg("status_bits").over(w_grp).alias("status_bits_avg_30m"),
        F.sum("temp_c").over(w2).alias("cumulative_temp_c"),
    )
)

base = (
    with_windows
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("firmware") != "1.0.3"))
    .select(
        "id",
        "temp_c",
        "status_bits",
        "firmware",
        "session_id",
        "status_bits_avg_30m",
        F.coalesce("status_bits_avg_30m", F.lit(0)).alias("status_bits_avg_30m_filled"),
    )
)

scored = (
    base
    .select(
        "*",
        F.ntile(10).over(w4).alias("status_bits_avg_30m_filled_bucket"),
        F.sum("session_id").over(w5).alias("running_session_id"),
        F.sum("temp_c").over(w6).alias("running_temp_c"),
        F.last("status_bits_avg_30m", ignorenulls=True).over(w7).alias("last_known_status_bits_avg_30m"),
    )
)

cleaned = (
    scored
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "*",
        F.greatest("session_id", "last_known_status_bits_avg_30m").alias("greatest_session_id_last_known_status_bits_avg_30m"),
        F.least("running_temp_c", "session_id").alias("least_running_temp_c_session_id"),
    )
)

report = (
    cleaned
    .select(
        "id",
        "temp_c",
        "firmware",
        "status_bits_avg_30m_filled",
        "running_session_id",
        "running_temp_c",
        "last_known_status_bits_avg_30m",
        "least_running_temp_c_session_id",
    )
    .orderBy("running_temp_c", "firmware")
)

result = (
    report
    .select(
        "id",
        "temp_c",
        "status_bits_avg_30m_filled",
        "running_temp_c",
        "least_running_temp_c_session_id",
    )
    .orderBy("status_bits_avg_30m_filled", "temp_c")
)
