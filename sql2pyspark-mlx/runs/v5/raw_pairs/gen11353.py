from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-2, Window.currentRow)

metrics = (
    df_device_readings
    .select(
        "id",
        "humidity",
        "status_bits",
        "firmware",
        F.avg("humidity").over(w).alias("last3_avg_humidity"),
        F.count("humidity").over(w).alias("last3_count_humidity"),
        F.min("humidity").over(w).alias("last3_min_humidity"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("humidity") >= 78, "PREMIUM").otherwise("STANDARD").alias("humidity_tier"),
    )
    .agg(
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.round(F.avg("last3_min_humidity"), 2).alias("avg_last3_min_humidity"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
