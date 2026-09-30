from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")
w_running = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("device_code")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("temp_c") * 1.07), 2).alias("temp_c_scaled"),
        F.year("reading_ts").alias("reading_ts_year"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .withColumn("humidity_rank", F.rank().over(w_running))
    .withColumn("pressure_avg_5", F.avg("pressure").over(w3))
    .withColumn("device_code_min_humidity", F.min("humidity").over(w4))
)

result = (
    enriched
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.max("reading_ts").alias("last_seen"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity_rank"))).alias("dv_02_total_humidity_rank"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("pressure_avg_5")).otherwise(0)).alias("dv_03_pressure_avg_5"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("dv_03_pressure_avg_5") > 0)
    .orderBy(F.col("dv_03_pressure_avg_5").desc_nulls_last())
)
