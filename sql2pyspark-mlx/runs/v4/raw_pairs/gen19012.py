from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status_bits_tier").orderBy(F.col("dv_04_pressure").desc())

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("status_bits") >= 11, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
        "device_code",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.min("reading_ts").alias("first_seen"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("pressure")).otherwise(0)).alias("dv_04_pressure"),
    )
    .withColumn("dv_04_pressure_rank", F.rank().over(w))
    .orderBy(F.col("row_count").asc_nulls_last())
)
