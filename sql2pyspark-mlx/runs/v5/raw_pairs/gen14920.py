from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("device_code") != "DV-01")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("device_code").isin(["DV-02", "DV-01"]), "Tier 1").otherwise("Other").alias("device_code_group"),
        (F.col("pressure") * F.col("temp_c")).alias("pressure_times_temp_c"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "temp_c",
        "firmware",
        "device_code_group",
        "pressure_times_temp_c",
        F.min("humidity").over(w).alias("last4_min_humidity"),
        F.count("humidity").over(w).alias("last4_count_humidity"),
        F.avg("humidity").over(w).alias("last4_avg_humidity"),
        F.sum("humidity").over(w).alias("last4_sum_humidity"),
        F.sum("pressure_times_temp_c").over(w2).alias("pressure_times_temp_c_sum_2h"),
    )
)

base = (
    enriched
    .filter(F.col("pressure_times_temp_c_sum_2h") >= 196)
    .select(
        "id",
        "firmware",
        "device_code_group",
        "pressure_times_temp_c",
        "last4_min_humidity",
        "last4_count_humidity",
        "last4_avg_humidity",
        "last4_sum_humidity",
        "pressure_times_temp_c_sum_2h",
        F.when(F.col("temp_c") >= 37.9, "High").when(F.col("temp_c") >= 21.7, "Medium").otherwise("Low").alias("temp_c_band"),
        F.when(F.col("last4_min_humidity") >= 77, "High").when(F.col("last4_min_humidity") >= 46, "Medium").otherwise("Low").alias("last4_min_humidity_tier"),
    )
)

remaining_keys = (
    base
    .select("firmware", "id")
    .subtract(
        df_device_readings
        .filter(~F.col("device_code").isin(["DV-03", "DV-02"]))
        .select("firmware", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("last4_count_humidity") >= 16, "LARGE").otherwise("SMALL").alias("last4_count_humidity_tier"),
    )
    .agg(
        F.round(F.avg("last4_avg_humidity"), 2).alias("avg_last4_avg_humidity"),
        F.count("*").alias("num_rows"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
    )
)
