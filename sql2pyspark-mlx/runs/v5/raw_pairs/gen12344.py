from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code")

with_windows = (
    df_device_readings
    .filter(F.col("status_bits").between(3, 6))
    .withColumn("next_humidity", F.lead("humidity").over(w))
    .withColumn("prev_temp_c", F.lag("temp_c").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("device_code_count_pressure", F.count("pressure").over(w2))
)

result = (
    with_windows
    .withColumn("temp_c_tier", F.when(F.col("temp_c") >= 41.6, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["device_code", "temp_c_tier"], ["device_code"], []],
        "device_code",
        "temp_c_tier",
    )
    .agg(
        F.max("reading_ts").alias("last_reading_ts"),
        F.corr("humidity", "pressure").alias("corr_humidity_pressure"),
        F.min("status_bits").alias("min_status_bits"),
    )
    .orderBy("device_code")
)
