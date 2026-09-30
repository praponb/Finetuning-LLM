from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("humidity", "id")
w9 = Window.partitionBy("device_code")
w10 = Window.partitionBy("device_code").orderBy(F.col("prev_reading_ts").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w11 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy(F.col("prev_humidity").desc(), "id")

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

metrics = (
    islands
    .withColumn("last_known_is_new_burst", F.last("is_new_burst", ignorenulls=True).over(w_grp))
    .withColumn("first_known_pressure", F.first("pressure", ignorenulls=True).over(w4))
    .withColumn("status_bits_max_2h", F.max("status_bits").over(w5))
)

enriched = (
    metrics
    .withColumn("is_new_burst_count_1h", F.count("is_new_burst").over(w6))
    .withColumn("burst_id_avg_2h", F.avg("burst_id").over(w7))
    .withColumn("prev_humidity", F.lag("humidity", 1, 0).over(w8))
    .withColumn("device_code_stddev_samp_pressure", F.stddev_samp("pressure").over(w9))
    .withColumn("last_known_is_new_burst_max_90d", F.max("last_known_is_new_burst").over(w10))
)

filtered = (
    enriched
    .filter(F.col("prev_humidity") <= 73)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "prev_reading_ts",
        "burst_id",
        "last_known_is_new_burst",
        "status_bits_max_2h",
        "is_new_burst_count_1h",
        "burst_id_avg_2h",
        "prev_humidity",
        F.when(F.col("firmware") == "1.1.0", 1).otherwise(0).alias("is_1_1_0"),
    )
)

filtered_device_readings = (
    filtered
    .filter(F.col("temp_c").between(35, 38))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "burst_id",
        "status_bits_max_2h",
        "is_new_burst_count_1h",
        "burst_id_avg_2h",
        "prev_humidity",
        "is_1_1_0",
        (F.col("status_bits") * F.col("humidity")).alias("status_bits_times_humidity"),
    )
)

filtered_device_readings_2 = (
    filtered_device_readings
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & (F.col("pressure") < 1023))
    .withColumn("is_1_1_0_rounded", F.round("is_1_1_0", 0))
)

cleaned = (
    filtered_device_readings_2
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select("*", F.ceil("is_1_1_0").alias("is_1_1_0_whole"))
)

result = (
    cleaned
    .select(
        "*",
        F.first("is_1_1_0", ignorenulls=True).over(w11).alias("first_known_is_1_1_0"),
        F.first("is_1_1_0_rounded", ignorenulls=True).over(w_running).alias("first_known_is_1_1_0_rounded"),
        F.ntile(3).over(w_ordered).alias("prev_humidity_bucket"),
    )
)
