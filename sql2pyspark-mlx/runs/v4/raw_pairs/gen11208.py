from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("temp_c") >= 36.3, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
    )
    .agg(
        F.max("reading_ts").alias("ended_at"),
        F.count("*").alias("num_rows"),
    )
)
