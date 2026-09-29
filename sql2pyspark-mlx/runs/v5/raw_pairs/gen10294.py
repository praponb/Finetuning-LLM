from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        "island_id",
    )
    .agg(
        F.avg("is_new_session").alias("avg_is_new_session"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("island_id")).otherwise(0)).alias("dv_02_island_id"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("is_new_session"))).alias("dv_01_total_is_new_session"),
    )
)
