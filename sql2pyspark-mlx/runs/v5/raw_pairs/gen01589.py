from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last(), "id")

deduped = (
    df_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.percentile_approx("humidity", 0.5).alias("humidity_p50"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("humidity")).otherwise(0)).alias("dv_04_humidity"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
    )
    .orderBy(F.col("dv_04_humidity").desc_nulls_first())
)
