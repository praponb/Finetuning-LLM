from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c_int", "id")
w2 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("device_code").orderBy(F.col("seq_no_scaled_minus_status_bits").asc_nulls_last())
w5 = Window.orderBy("humidity", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.col("pressure") < 1006))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.col("temp_c").cast("int").alias("temp_c_int"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lead("pressure").over(w).alias("next_pressure"),
        F.lag("status_bits").over(w).alias("prev_status_bits"),
        F.row_number().over(w).alias("seq_no"),
    )
)

cleaned_device_readings = (
    enriched
    .filter(F.col("reading_ts") > F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .withColumn("firmware_device_code_key", F.concat_ws("-", "firmware", "device_code"))
    .withColumn("seq_no_scaled", F.round((F.col("seq_no") * 0.85), 2))
)

valid = (
    cleaned_device_readings
    .filter(F.col("prev_status_bits") != 12)
    .withColumn("seq_no_scaled_minus_status_bits", (F.col("seq_no_scaled") - F.col("status_bits")))
    .withColumn("status_bits_scaled", F.round((F.col("status_bits") * 100), 2))
)

with_windows = (
    valid
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        "prev_status_bits",
        "firmware_device_code_key",
        "seq_no_scaled_minus_status_bits",
        "status_bits_scaled",
        F.sum("pressure").over(w2).alias("running_pressure"),
        F.avg("seq_no").over(w3).alias("seq_no_avg_2_prior"),
        F.cume_dist().over(w4).alias("seq_no_scaled_minus_status_bits_cume_dist"),
        F.avg("pressure").over(w5).alias("pressure_avg_3"),
    )
)

totals = (
    with_windows
    .withColumn("seq_no_avg_2_prior_tier", F.when(F.col("seq_no_avg_2_prior") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["seq_no_avg_2_prior_tier", "device_code"], ["seq_no_avg_2_prior_tier"], []],
        "seq_no_avg_2_prior_tier",
        "device_code",
    )
    .agg(
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("status_bits_per_dv_04"),
        F.sum("seq_no_scaled_minus_status_bits_cume_dist").alias("total_seq_no_scaled_minus_status_bits_cume_dist"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("total_seq_no_scaled_minus_status_bits_cume_dist") > 0)
)

result = (
    totals
    .select(
        "seq_no_avg_2_prior_tier",
        "device_code",
        "status_bits_per_dv_04",
        "humidity_stddev",
        "distinct_ids",
    )
    .orderBy(F.col("device_code").desc_nulls_last())
)
