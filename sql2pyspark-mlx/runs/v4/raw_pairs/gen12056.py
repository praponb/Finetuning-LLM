from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("activity_day").orderBy(F.col("status_bits_bit_or").desc())

metrics = (
    df_device_readings
    .withColumn("corr_humidity_status_bits", F.corr("humidity", "status_bits").over(w))
    .withColumn("last_known_temp_c", F.last("temp_c", ignorenulls=True).over(w2))
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w3))
)

result = (
    metrics
    .groupBy(
        F.date_trunc("day", "reading_ts").alias("activity_day"),
        F.when(F.col("pressure") >= 1019, "LARGE").otherwise("SMALL").alias("pressure_tier"),
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.count("*").alias("cnt"),
    )
    .withColumn("status_bits_bit_or_percent_rank", F.percent_rank().over(w4))
)
