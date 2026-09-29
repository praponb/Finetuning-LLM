from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("activity_day").orderBy(F.col("dv_03_events").desc())

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("pressure") >= 1019, "PREMIUM").otherwise("STANDARD").alias("pressure_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
    )
    .withColumn("dv_03_events_percent_rank", F.percent_rank().over(w3))
    .orderBy("temp_c_stddev")
)
