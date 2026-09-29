from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_dv_01").desc_nulls_last())
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("is_dv_01", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("device_code") == "DV-02")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("device_code") == "DV-01", 1).otherwise(0).alias("is_dv_01"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w).alias("is_dv_01_percent_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("pressure").over(w3).alias("running_pressure"),
    )
)

matched_keys = (
    metrics
    .select("device_code")
    .intersect(
        df_device_readings
        .filter(F.col("humidity") <= 63)
        .select("device_code")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & F.col("running_pressure").between(4050, 4159))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "is_dv_01_percent_rank",
        "event_seq",
        "running_pressure",
        F.when(F.col("device_code") == "DV-04", 1).otherwise(0).alias("is_dv_04"),
    )
)

filtered = (
    valid
    .filter(F.col("device_code") != "DV-04")
    .select(
        "*",
        F.round("is_dv_04", 1).alias("is_dv_04_rounded"),
        (F.col("is_dv_04") * F.col("running_pressure")).alias("is_dv_04_times_running_pressure"),
    )
)

base = (
    filtered
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("device_code") == "DV-03"))
    .select(
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "is_dv_01_percent_rank",
        "event_seq",
        "is_dv_04",
        "is_dv_04_times_running_pressure",
        F.when(F.col("pressure") >= 1017, "High").when(F.col("pressure") >= 1004, "Medium").otherwise("Low").alias("pressure_band"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("status_bits") >= 9, "HIGH").otherwise("LOW").alias("status_bits_tier"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.avg(F.when(F.col("device_code") == "DV-02", F.col("is_dv_04"))).alias("dv_02_avg_is_dv_04"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("is_dv_01_percent_rank")).otherwise(0)).alias("dv_02_is_dv_01_percent_rank"),
        F.avg(F.when(F.col("device_code") == "DV-03", F.col("event_seq"))).alias("dv_03_avg_event_seq"),
    )
)
