from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_first(), "id")
w2 = Window.orderBy(F.col("pressure_minus_temp_c").desc())

valid = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-04", "DV-03"]))
    .select(
        "*",
        F.concat("firmware", F.lit(", "), "device_code").alias("firmware_device_code"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

remaining_keys = (
    deduped
    .select("device_code", "firmware")
    .subtract(
        df_device_readings
        .filter(F.col("device_code").isin(["DV-02", "DV-03"]))
        .select("device_code", "firmware")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware_device_code",
        (F.col("pressure") - F.col("temp_c")).alias("pressure_minus_temp_c"),
        F.hour("reading_ts").alias("reading_ts_hour"),
    )
)

scored = base.select("*", F.rank().over(w2).alias("pressure_minus_temp_c_rank"))

summary = (
    scored
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.max("reading_ts_hour").alias("max_reading_ts_hour"),
        F.percentile_approx("reading_ts_hour", [0.25, 0.5, 0.75]).alias("reading_ts_hour_quartiles"),
    )
)

result = summary.select("activity_day", "max_reading_ts_hour", "reading_ts_hour_quartiles")
