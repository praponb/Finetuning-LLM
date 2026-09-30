from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device_code")
w_hist = Window.partitionBy("device_code").orderBy("humidity")
w5 = Window.orderBy(F.col("temp_c").desc_nulls_last())

flagged = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

remaining_keys = (
    sessionized
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") == "1.1.0")
        .select("id")
    )
)

kept_rows = (
    sessionized
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 01:00:00").cast("timestamp"))
    .select(
        "*",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.corr("pressure", "temp_c").over(w_ordered).alias("corr_pressure_temp_c"),
        F.rank().over(w_hist).alias("humidity_rank"),
        F.sum("days_since_reading_ts").over(w_ordered).alias("device_code_sum_days_since_reading_ts"),
        F.dense_rank().over(w5).alias("temp_c_dense_rank"),
        F.max("temp_c").over(w_ordered).alias("device_code_max_temp_c"),
    )
)

cleaned = (
    metrics
    .filter(F.col("temp_c") > 32)
    .select(
        "reading_ts",
        "temp_c",
        "pressure",
        "session_id",
        "days_since_reading_ts",
        "corr_pressure_temp_c",
        "humidity_rank",
        "temp_c_dense_rank",
        F.floor("temp_c_dense_rank").alias("temp_c_dense_rank_whole"),
    )
)

cleaned_device_readings = (
    cleaned
    .filter(F.col("corr_pressure_temp_c") <= 0.2)
    .select(
        "reading_ts",
        "temp_c",
        "pressure",
        "session_id",
        "days_since_reading_ts",
        "humidity_rank",
        "temp_c_dense_rank_whole",
        (F.col("corr_pressure_temp_c") * F.col("temp_c")).alias("corr_pressure_temp_c_times_temp_c"),
    )
)

valid = (
    cleaned_device_readings
    .filter(F.col("humidity_rank") == 3)
    .select(
        "temp_c",
        "pressure",
        "temp_c_dense_rank_whole",
        "corr_pressure_temp_c_times_temp_c",
        F.least("temp_c", "humidity_rank").alias("least_temp_c_humidity_rank"),
    )
)

result = (
    valid
    .select(
        "temp_c",
        "temp_c_dense_rank_whole",
        "corr_pressure_temp_c_times_temp_c",
        "least_temp_c_humidity_rank",
    )
)
