from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("dv_01_count").desc())

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        "device_code",
        F.when(F.col("new_group_flag") >= 1, "HIGH").otherwise("LOW").alias("new_group_flag_tier"),
    )
    .agg(
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
        F.avg("pressure").alias("avg_pressure"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("dv_01_count") > 1)
    .withColumn("dv_01_count_percent_rank", F.percent_rank().over(w3))
)
