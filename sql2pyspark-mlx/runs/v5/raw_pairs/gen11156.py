from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("device_code")
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("status_bits")).otherwise(0)).alias("dv_01_status_bits"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.percentile_approx("status_bits", 0.9).alias("status_bits_p90"),
        F.percentile_approx("new_group_flag", 0.9).alias("new_group_flag_p90"),
        F.count("*").alias("cnt"),
    )
)
