from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("humidity").asc_nulls_first())
w3 = Window.orderBy("pressure", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_device_readings
    .select(
        "*",
        F.avg("pressure").over(w).alias("pressure_avg_2"),
        F.cume_dist().over(w2).alias("humidity_cume_dist"),
        F.avg("temp_c").over(w3).alias("temp_c_avg_6"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "humidity_cume_dist",
        F.when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("device_code_zone"),
    )
    .orderBy("firmware")
)
