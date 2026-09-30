from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("burst_id_tier").orderBy(F.col("pressure_per_dv_04").desc())

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        F.when(F.col("temp_c") >= 34.6, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
        "device_code",
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.corr("humidity", "pressure").alias("corr_humidity_pressure"),
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("pressure_per_dv_04"),
    )
    .withColumn("pressure_per_dv_04_dense_rank", F.dense_rank().over(w3))
)
