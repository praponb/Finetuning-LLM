from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.orderBy(F.col("humidity").asc_nulls_last())
w4 = Window.partitionBy("device_code")
w_recent = Window.partitionBy("device_code").orderBy("status_bits")
w6 = Window.partitionBy("humidity_cume_dist_tier").orderBy(F.col("avg_humidity").desc())

scored = (
    df_device_readings
    .select(
        "*",
        F.ntile(5).over(w_hist).alias("humidity_bucket"),
        F.avg("temp_c").over(w2).alias("temp_c_avg_6"),
        F.cume_dist().over(w_grp).alias("humidity_cume_dist"),
        F.corr("pressure", "status_bits").over(w4).alias("corr_pressure_status_bits"),
        F.percent_rank().over(w_recent).alias("status_bits_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("humidity_cume_dist") >= 0.7, "PREMIUM").otherwise("STANDARD").alias("humidity_cume_dist_tier"),
        "device_code",
    )
    .agg(
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.percentile_approx("humidity_bucket", [0.25, 0.5, 0.75]).alias("humidity_bucket_quartiles"),
    )
    .withColumn("avg_humidity_rank", F.rank().over(w6))
    .orderBy(F.col("avg_humidity").desc())
)
