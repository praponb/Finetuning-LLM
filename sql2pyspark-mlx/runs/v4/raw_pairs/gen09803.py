from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first(), "id")
w2 = Window.orderBy(F.col("humidity").desc())
w_hist = Window.orderBy(F.col("pressure").desc_nulls_first())
w4 = Window.partitionBy("device_code")
w5 = Window.orderBy(F.col("temp_c").desc_nulls_first())
w6 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

deduped = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    deduped
    .select("firmware")
    .intersectAll(
        df_device_readings
        .filter(F.col("device_code").isin(["DV-02", "DV-04", "DV-01"]))
        .select("firmware")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("status_bits") > 5)
    .withColumn("device_code_firmware", F.concat("device_code", F.lit(", "), "firmware"))
)

scored = (
    valid
    .select(
        "*",
        F.dense_rank().over(w2).alias("humidity_dense_rank"),
        F.rank().over(w_hist).alias("pressure_rank"),
        F.sum("pressure").over(w4).alias("device_code_sum_pressure"),
    )
)

cleaned = (
    scored
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        F.round((F.col("device_code_sum_pressure") / F.col("pressure_rank")), 2).alias("device_code_sum_pressure_per_pressure_rank"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.corr("status_bits", "temp_c").over(w4).alias("corr_status_bits_temp_c"),
        F.percent_rank().over(w5).alias("temp_c_percent_rank"),
        F.max("device_code_sum_pressure").over(w6).alias("device_code_sum_pressure_max_15m"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "rn",
        "device_code_firmware",
    )
    .orderBy(F.col("temp_c").desc())
)
