from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("humidity").between(41, 73))
    .withColumn("days_since_reading_ts", F.datediff(F.current_date(), "reading_ts"))
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity")).otherwise(0)).alias("dv_02_humidity"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.round(F.sum("days_since_reading_ts") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("days_since_reading_ts_per_dv_01"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.min("is_new_burst").alias("min_is_new_burst"),
        F.percentile_approx("days_since_reading_ts", [0.25, 0.5, 0.75]).alias("days_since_reading_ts_quartiles"),
    )
    .filter(F.col("dv_02_humidity") >= 0)
)
