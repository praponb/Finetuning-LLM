from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .groupingSets(
        [["device_code", "activity_day"], ["device_code"], []],
        "device_code",
        "activity_day",
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("status_bits")).otherwise(0)).alias("dv_03_status_bits"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.min("reading_ts").alias("first_seen"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("dv_01_count") < 1)
    .orderBy(F.col("activity_day").asc_nulls_last())
)
