from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

ranked = (
    df_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    ranked
    .select("id", "device_code")
    .intersectAll(
        df_device_readings
        .filter(F.hour("reading_ts").between(8, 14))
        .select("id", "device_code")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    verified
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
        .select("firmware", "device_code")
    )
)

verified_device_readings = (
    verified
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

base = (
    verified_device_readings
    .filter(F.col("pressure") < 1016)
    .withColumn("humidity_rounded", F.round("humidity", 1))
)

with_windows = (
    base
    .withColumn("humidity_rounded_sum_30m", F.sum("humidity_rounded").over(w2))
    .withColumn("humidity_rounded_max_2h", F.max("humidity_rounded").over(w3))
    .withColumn("pressure_count_1h", F.count("pressure").over(w_part))
)

valid = (
    with_windows
    .filter(F.hour("reading_ts").between(11, 17))
    .select(
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "humidity_rounded",
        "humidity_rounded_sum_30m",
        "humidity_rounded_max_2h",
        "pressure_count_1h",
        (F.col("pressure_count_1h") - F.col("humidity_rounded_max_2h")).alias("pressure_count_1h_minus_humidity_rounded_max_2h"),
    )
)

base_device_readings = (
    valid
    .filter(F.col("firmware") == "2.0.1")
    .select(
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "humidity_rounded",
        "humidity_rounded_sum_30m",
        "pressure_count_1h",
        "pressure_count_1h_minus_humidity_rounded_max_2h",
        F.floor("status_bits").alias("status_bits_whole"),
    )
)

result = (
    base_device_readings
    .select(
        "reading_ts",
        "temp_c",
        "humidity",
        "humidity_rounded",
        "humidity_rounded_sum_30m",
        "pressure_count_1h",
        "status_bits_whole",
    )
    .orderBy(F.col("pressure_count_1h").desc())
)
