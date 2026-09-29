from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("humidity") > 48)
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1031, "High").when(F.col("pressure") >= 989, "Medium").otherwise("Low"))
    .withColumn("device_code_upper", F.upper("device_code"))
)

result = (
    valid
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("pressure", 0.95).alias("pressure_p95"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
    )
    .filter(F.col("dv_04_count") > 1)
)
