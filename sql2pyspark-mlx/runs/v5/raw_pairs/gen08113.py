from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("corr_humidity_status_bits").desc())

flagged = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy("session_id", "device_code")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.corr("humidity", "status_bits").alias("corr_humidity_status_bits"),
    )
    .withColumn("corr_humidity_status_bits_rank", F.rank().over(w3))
    .orderBy("corr_humidity_status_bits")
)
