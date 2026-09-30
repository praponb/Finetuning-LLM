from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

ranked = (
    df_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.percentile_approx("humidity", 0.9).alias("humidity_p90"),
        F.max("reading_ts").alias("last_seen"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
    )
)
