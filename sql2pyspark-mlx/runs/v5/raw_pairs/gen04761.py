from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("device_code")
w_running = Window.orderBy("status_bits")
w3 = Window.orderBy(F.col("humidity").asc_nulls_last())

cleaned = (
    df_device_readings
    .filter(F.to_date("reading_ts") >= "2025-02-01")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round("humidity", 1).alias("humidity_rounded"),
    )
)

kept_keys = (
    cleaned
    .select("firmware")
    .subtract(
        df_device_readings
        .filter(F.col("pressure") < 999)
        .select("firmware")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("device_code_count_temp_c", F.count("temp_c").over(w_ordered))
    .withColumn("device_code_count_humidity", F.count("humidity").over(w_ordered))
)

base = (
    enriched
    .filter(F.col("humidity_rounded").between(27, 45))
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "device_code_count_temp_c",
        "device_code_count_humidity",
        F.when(F.col("humidity") >= 78, "High").when(F.col("humidity") >= 30, "Medium").otherwise("Low").alias("humidity_level"),
    )
)

kept_keys_2 = (
    base
    .select("firmware")
    .intersectAll(
        df_device_readings
        .filter(F.col("status_bits") <= 9)
        .select("firmware")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("device_code_count_temp_c").isNotNull())
    .withColumn("humidity_minus_temp_c", (F.col("humidity") - F.col("temp_c")))
    .withColumn("humidity_times_status_bits", (F.col("humidity") * F.col("status_bits")))
)

metrics = (
    valid
    .select(
        "id",
        "temp_c",
        "humidity",
        "device_code_count_temp_c",
        "device_code_count_humidity",
        "humidity_level",
        "humidity_minus_temp_c",
        F.percent_rank().over(w_running).alias("status_bits_percent_rank"),
        F.rank().over(w_running).alias("status_bits_rank"),
        F.dense_rank().over(w3).alias("humidity_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("humidity_minus_temp_c") >= 84, "LARGE").otherwise("SMALL").alias("humidity_minus_temp_c_tier"),
    )
    .agg(
        F.percentile_approx("status_bits_percent_rank", [0.25, 0.5, 0.75]).alias("status_bits_percent_rank_quartiles"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.round(F.avg("status_bits_rank"), 2).alias("avg_status_bits_rank"),
        F.round(F.avg("status_bits_rank"), 2).alias("avg_status_bits_rank_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("avg_status_bits_rank") < 1000000)
    .orderBy(F.col("humidity_minus_temp_c_tier").desc())
)
