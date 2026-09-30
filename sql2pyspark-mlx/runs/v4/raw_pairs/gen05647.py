from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id")
w2 = Window.orderBy(F.col("pressure_per_temp_c").desc(), "id")
w3 = Window.orderBy(F.col("humidity").desc())
w4 = Window.orderBy("next_pressure_per_temp_c", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("humidity_dense_rank")
w7 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.orderBy(F.col("last_known_pressure_per_temp_c").desc(), "id")
w9 = Window.orderBy("last_known_pressure_per_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("temp_c").between(25, 34))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        F.round((F.col("pressure") / F.col("temp_c")), 2).alias("pressure_per_temp_c"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.lead("pressure_per_temp_c", 1).over(w).alias("next_pressure_per_temp_c"),
        F.row_number().over(w2).alias("seq_no"),
        F.dense_rank().over(w3).alias("humidity_dense_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("pressure_per_temp_c").between(23, 46))
    .select(
        "*",
        (F.col("humidity_dense_rank") + F.col("pressure_per_temp_c")).alias("humidity_dense_rank_plus_pressure_per_temp_c"),
    )
)

scored = (
    valid
    .withColumn("humidity_dense_rank_avg_5_prior", F.avg("humidity_dense_rank").over(w4))
    .withColumn("last_known_pressure_per_temp_c", F.last("pressure_per_temp_c", ignorenulls=True).over(w5))
)

metrics_device_readings = (
    scored
    .withColumn("humidity_dense_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("temp_c_sum_30m", F.sum("temp_c").over(w7))
    .withColumn("last_known_pressure_per_temp_c_bucket", F.ntile(10).over(w_recent))
    .withColumn("running_pressure_per_temp_c", F.sum("pressure_per_temp_c").over(w9))
    .withColumn("next_pressure_per_temp_c_avg_1h", F.avg("next_pressure_per_temp_c").over(w_hist))
)

result = (
    metrics_device_readings
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("temp_c") >= 39.6, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
    )
    .agg(
        F.sum("last_known_pressure_per_temp_c_bucket").alias("total_last_known_pressure_per_temp_c_bucket"),
        F.percentile_approx("temp_c", 0.5).alias("temp_c_p50"),
        F.percentile_approx("last_known_pressure_per_temp_c", 0.9).alias("last_known_pressure_per_temp_c_p90"),
        F.count("*").alias("event_count"),
        F.max("reading_ts").alias("last_reading_ts"),
    )
    .orderBy("total_last_known_pressure_per_temp_c_bucket")
)
