from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")

latest = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy("device_code", F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.percentile_approx("status_bits", 0.95).alias("status_bits_p95"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.percentile_approx("humidity", 0.5).alias("humidity_p50"),
        F.avg(F.when(F.col("device_code") == "DV-01", F.col("humidity"))).alias("dv_01_avg_humidity"),
    )
    .orderBy(F.col("dv_02_events").desc())
)
