from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id")

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 13:00:00").cast("timestamp")) & (F.col("status_bits") > 8))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        F.when(F.col("status_bits") >= 11, "High").when(F.col("status_bits") >= 5, "Medium").otherwise("Low").alias("status_bits_tier"),
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

base = (
    cleaned
    .filter(F.col("temp_c") >= 38)
    .withColumn("pressure_whole", F.ceil("pressure"))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    ranked
    .select("id", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("device_code") == "DV-02")
        .select("id", "device_code")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits_tier",
        "pressure_whole",
        F.when(F.col("device_code").rlike("^(DV-04|DV-02)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("device_code_zone"),
    )
    .orderBy(F.col("pressure_whole").desc())
)
