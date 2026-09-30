from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .groupingSets(
        [["device_code", "hour_window"], ["device_code"], []],
        "device_code",
        "hour_window",
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("humidity").alias("avg_humidity"),
        F.grouping("device_code").alias("grouping_level"),
    )
    .orderBy(F.col("hour_window").asc_nulls_last())
)
