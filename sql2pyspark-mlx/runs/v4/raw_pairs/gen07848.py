from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("status_bits_plus_pressure").desc_nulls_first())
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp")) & (F.col("humidity") >= 75))
    .withColumn("status_bits_plus_pressure", (F.col("status_bits") + F.col("pressure")))
)

metrics = (
    valid
    .select(
        "*",
        F.sum("temp_c").over(w).alias("running_temp_c"),
        F.rank().over(w2).alias("status_bits_plus_pressure_rank"),
        F.ntile(4).over(w3).alias("status_bits_bucket"),
    )
)

matched_keys = (
    metrics
    .select("device_code")
    .intersect(
        df_device_readings
        .filter(F.to_date("reading_ts") < "2025-02-01")
        .select("device_code")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube(
        F.when(F.col("running_temp_c") >= 148, "HIGH").otherwise("LOW").alias("running_temp_c_tier"),
        "device_code",
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity")).otherwise(0)).alias("dv_02_humidity"),
        F.max("reading_ts").alias("ended_at"),
    )
    .filter(F.col("dv_02_humidity") > 0)
    .orderBy(F.col("running_temp_c_tier").asc_nulls_last())
)
