from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "firmware",
        (F.col("humidity") / F.nullif("status_bits", F.lit(0))).alias("humidity_per_status_bits"),
    )
)

valid = (
    base
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 09:00:00").cast("timestamp")) & ~F.col("device_code").isin(["DV-02", "DV-01", "DV-04"]))
    .select("*", F.upper("device_code").alias("device_code_upper"))
)

cleaned = (
    valid
    .filter(F.to_date("reading_ts") < "2025-08-01")
    .withColumn("firmware_length", F.length("firmware"))
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

matched_keys = (
    sessions_device_readings
    .select("device_code", "firmware")
    .intersect(
        df_device_readings
        .filter(F.col("device_code") == "DV-04")
        .select("device_code", "firmware")
    )
)

verified = (
    sessions_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "device_code",
        "reading_ts",
        "pressure",
        "firmware",
        "humidity_per_status_bits",
        "device_code_upper",
        "session_id",
        F.when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("device_code_zone"),
    )
)
