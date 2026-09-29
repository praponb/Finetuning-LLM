from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.partitionBy("device_code")
w3 = Window.orderBy("status_bits", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

enriched = (
    df_device_readings
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("corr_pressure_temp_c", F.corr("pressure", "temp_c").over(w2))
    .withColumn("status_bits_avg_6_prior", F.avg("status_bits").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

result = (
    enriched
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "status_bits_avg_6_prior",
        "event_seq",
    )
    .orderBy("humidity", F.col("temp_c").desc())
)
