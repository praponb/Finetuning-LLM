from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("status_bits")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")

scored = (
    df_device_readings
    .filter(F.col("temp_c") >= 34)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        F.stddev_samp("pressure").over(w).alias("device_code_stddev_samp_pressure"),
        F.dense_rank().over(w2).alias("status_bits_dense_rank"),
        F.lag("temp_c", 1, 0).over(w3).alias("prev_temp_c"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_bits_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("status_bits_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("humidity", 0.9).alias("humidity_p90"),
        F.sum("device_code_stddev_samp_pressure").alias("total_device_code_stddev_samp_pressure"),
    )
    .filter(F.col("total_device_code_stddev_samp_pressure") > 0)
    .orderBy(F.col("cnt").desc())
)
