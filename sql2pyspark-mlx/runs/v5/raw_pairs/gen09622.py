from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts_day", "id")
w3 = Window.orderBy(F.col("humidity").desc())
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w5 = Window.orderBy("temp_c", "id").rowsBetween(-2, -1)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp")) & (F.col("firmware") == "2.0.1"))
    .withColumn("reading_ts_day", F.col("reading_ts").cast("date"))
    .withColumn("reading_ts_label", F.date_format("reading_ts", "yyyy-MM-dd"))
)

result = (
    valid
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits",
        "reading_ts_day",
        F.lead("humidity", 2).over(w).alias("next_humidity"),
        F.lead("temp_c", 1, 0).over(w2).alias("next_temp_c"),
        F.dense_rank().over(w3).alias("humidity_dense_rank"),
        F.ntile(4).over(w4).alias("temp_c_bucket"),
        F.avg("humidity").over(w5).alias("humidity_avg_2_prior"),
    )
)
