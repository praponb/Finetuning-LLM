from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .filter(F.col("device_code") == "DV-02")
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "firmware",
        F.avg("humidity").over(w).alias("running_avg_humidity"),
        F.min("humidity").over(w).alias("running_min_humidity"),
        F.count("humidity").over(w).alias("running_count_humidity"),
        F.last("humidity", ignorenulls=True).over(w2).alias("last_known_humidity"),
    )
)

matched_keys = (
    enriched
    .select("firmware")
    .intersect(
        df_device_readings
        .filter(F.col("device_code") != "DV-04")
        .select("firmware")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("running_count_humidity") >= 7)
    .select(
        "id",
        "pressure",
        "status_bits",
        "firmware",
        "running_min_humidity",
        "running_count_humidity",
        "last_known_humidity",
        F.when(F.col("firmware") == "2.0.1", 1).otherwise(0).alias("is_2_0_1"),
    )
)

filtered = (
    cleaned
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "id",
        "pressure",
        "status_bits",
        "running_count_humidity",
        "is_2_0_1",
        F.col("pressure").cast("int").alias("pressure_int"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .exceptAll(
        df_device_readings
        .filter(F.col("temp_c") >= 30)
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "pressure", "status_bits", "running_count_humidity", "is_2_0_1")
    .orderBy(F.col("pressure").desc_nulls_last())
)
