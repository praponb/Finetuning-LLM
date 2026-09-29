from pyspark.sql import functions as F

result = (
    df_device_readings
    .cube(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.min("reading_ts").alias("first_seen"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events_2"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("num_device_codes").desc())
)
