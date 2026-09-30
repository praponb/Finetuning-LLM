from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .cube(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("island_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("status_bits_per_dv_01"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.min("island_id").alias("min_island_id"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
    .filter(F.col("min_island_id") > 0)
    .orderBy(F.col("min_island_id").desc_nulls_last())
)
