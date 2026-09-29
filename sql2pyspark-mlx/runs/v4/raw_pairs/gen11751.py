from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("status_bits") == 10)
    .withColumn("temp_c_int", F.col("temp_c").cast("int"))
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.min("reading_ts").alias("first_seen"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("new_group_flag"))).alias("dv_04_total_new_group_flag"),
    )
)
