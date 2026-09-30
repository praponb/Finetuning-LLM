from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w_recent = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("humidity", "id")
w_part = Window.orderBy("humidity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("next_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 01:00:00"))) & (F.col("pressure") <= 1008))
    .withColumn("humidity_band", F.when(F.col("humidity") >= 81, "High").when(F.col("humidity") >= 40, "Medium").otherwise("Low"))
)

metrics = (
    valid
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "humidity_band",
        F.lead("status_bits", 1).over(w).alias("next_status_bits"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

scored = (
    metrics
    .withColumn("running_count_humidity", F.count("humidity").over(w_recent))
    .withColumn("running_min_humidity", F.min("humidity").over(w_recent))
    .withColumn("running_max_humidity", F.max("humidity").over(w_recent))
    .withColumn("running_avg_humidity", F.avg("humidity").over(w_recent))
    .withColumn("humidity_bucket", F.ntile(4).over(w4))
)

enriched = (
    scored
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "running_count_humidity",
        "running_max_humidity",
        "humidity_bucket",
        F.avg("humidity").over(w_part).alias("running_avg_humidity_2"),
        F.count("humidity").over(w_part).alias("running_count_humidity_2"),
        F.sum("humidity").over(w_part).alias("running_sum_humidity"),
        F.sum("status_bits").over(w6).alias("running_status_bits"),
    )
)

result = (
    enriched
    .select(
        "id",
        "pressure",
        "status_bits",
        "firmware",
        "running_count_humidity",
        "running_max_humidity",
        "humidity_bucket",
        "running_count_humidity_2",
    )
    .orderBy(F.col("firmware").desc())
)
