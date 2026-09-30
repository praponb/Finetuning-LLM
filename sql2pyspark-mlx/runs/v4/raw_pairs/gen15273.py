from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("humidity")).otherwise(0)).alias("dv_04_humidity"),
    )
    .filter(F.col("dv_04_humidity") < 0)
    .orderBy("dv_04_humidity")
)
