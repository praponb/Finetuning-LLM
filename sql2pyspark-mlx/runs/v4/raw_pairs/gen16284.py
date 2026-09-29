from pyspark.sql import functions as F

result = (
    df_device_readings
    .rollup(
        F.when(F.col("status_bits") >= 11, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
        F.when(F.col("humidity") >= 75, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("pressure_per_dv_02"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.max("temp_c").alias("max_temp_c"),
    )
    .filter(F.col("pressure_per_dv_02") > 0)
    .orderBy(F.col("humidity_tier").desc())
)
