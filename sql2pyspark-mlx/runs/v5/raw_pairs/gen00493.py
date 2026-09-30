from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("device_code")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.min("reading_ts").alias("started_at"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
    )
)
