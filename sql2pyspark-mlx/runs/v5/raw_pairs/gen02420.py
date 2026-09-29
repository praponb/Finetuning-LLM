from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code")

filtered = (
    df_device_readings
    .filter(F.col("status_bits") <= 5)
    .select(
        "*",
        F.substring("firmware", 1, 4).alias("firmware_prefix"),
        F.trim("device_code").alias("clean_device_code"),
    )
)

filtered_device_readings = (
    filtered
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & ~F.col("device_code").isin(["DV-04", "DV-01", "DV-03"]))
    .withColumn("device_code_firmware_key", F.concat_ws(",", "device_code", "firmware"))
)

cleaned = (
    filtered_device_readings
    .filter(F.col("device_code").isin(["DV-04", "DV-03"]))
    .withColumn("firmware_lower", F.lower("firmware"))
)

enriched = (
    cleaned
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "clean_device_code",
        "device_code_firmware_key",
        F.lag("pressure").over(w_recent).alias("prev_pressure"),
        F.lead("pressure").over(w_recent).alias("next_pressure"),
        F.lag("temp_c").over(w_recent).alias("prev_temp_c"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.sum("pressure").over(w2).alias("device_code_sum_pressure"),
    )
)

result = (
    enriched
    .cube(
        "device_code",
        F.when(F.col("device_code_sum_pressure") >= 3667, "PREMIUM").otherwise("STANDARD").alias("device_code_sum_pressure_tier"),
    )
    .agg(
        F.max(F.when(F.col("device_code") == "DV-02", F.col("prev_temp_c"))).alias("dv_02_max_prev_temp_c"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("device_code_sum_pressure"))).alias("dv_04_total_device_code_sum_pressure"),
        F.round(F.avg("device_code_sum_pressure"), 2).alias("avg_device_code_sum_pressure"),
    )
    .filter(F.col("avg_device_code_sum_pressure") >= 0)
)
