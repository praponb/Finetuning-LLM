from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 9, "LARGE").otherwise("SMALL"))
    .rollup("device_code", "status_bits_tier")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("status_bits_per_dv_01"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.grouping("device_code").alias("grouping_level"),
    )
    .orderBy(F.col("device_code").asc_nulls_first())
)
