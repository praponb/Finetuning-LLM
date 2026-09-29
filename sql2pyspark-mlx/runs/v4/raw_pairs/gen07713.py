from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy("device_code")
    .agg(
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
    )
)
