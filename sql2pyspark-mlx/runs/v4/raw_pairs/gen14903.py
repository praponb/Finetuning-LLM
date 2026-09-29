from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 15:00:00").cast("timestamp"))
    .withColumn("status_bits_rounded", F.round("status_bits", 2))
    .withColumn("pressure_rounded", F.round("pressure", 1))
)

kept_keys = (
    filtered
    .select("firmware")
    .subtract(
        df_device_readings
        .filter(F.col("pressure") < 1012)
        .select("firmware")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("device_code_max_temp_c", F.max("temp_c").over(w_part))
    .withColumn("corr_humidity_status_bits", F.corr("humidity", "status_bits").over(w_part))
    .withColumn("corr_pressure_status_bits", F.corr("pressure", "status_bits").over(w_part))
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("pressure_rounded") >= 85, "PREMIUM").otherwise("STANDARD").alias("pressure_rounded_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    stats
    .select("pressure_rounded_tier", "duration_min", "event_count", "distinct_ids")
    .orderBy(F.col("event_count").asc_nulls_first())
)
