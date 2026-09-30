from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("pressure").asc_nulls_last())
w5 = Window.partitionBy("device_code").orderBy("status_bits", "id")

metrics = (
    df_device_readings
    .withColumn("running_pressure", F.sum("pressure").over(w))
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w2))
    .withColumn("first_known_pressure", F.first("pressure", ignorenulls=True).over(w3))
    .withColumn("pressure_dense_rank", F.dense_rank().over(w4))
    .withColumn("next_temp_c", F.lead("temp_c", 1).over(w5))
)

result = (
    metrics
    .groupBy(F.date_trunc("day", "reading_ts").alias("activity_day"))
    .agg(
        F.max("reading_ts").alias("last_seen"),
        F.corr("pressure", "temp_c").alias("corr_pressure_temp_c"),
    )
    .filter(F.col("corr_pressure_temp_c") > 0)
    .orderBy("corr_pressure_temp_c")
)
