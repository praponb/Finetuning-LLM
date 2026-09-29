from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c_whole").asc_nulls_first(), "id")
w_hist = Window.partitionBy("device_code").orderBy("temp_c_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_first(), "id")

latest = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base = (
    latest
    .filter(F.col("device_code") == "DV-02")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
    )
)

cleaned = (
    base
    .filter(F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .withColumn("temp_c_whole", F.floor("temp_c"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .filter(F.col("rn") <= 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "firmware",
        "temp_c_whole",
        F.max("humidity").over(w_hist).alias("running_max_humidity"),
        F.avg("humidity").over(w_hist).alias("running_avg_humidity"),
        F.sum("humidity").over(w_hist).alias("running_sum_humidity"),
        F.last("humidity", ignorenulls=True).over(w4).alias("last_known_humidity"),
    )
)

latest_device_readings = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest_device_readings
    .select(
        "id",
        "device_code",
        F.col("last_known_humidity").alias("LAST_KNOWN_HUMIDITY"),
        F.col("temp_c").alias("TEMP_C"),
        F.col("running_max_humidity").alias("RUNNING_MAX_HUMIDITY_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["LAST_KNOWN_HUMIDITY", "TEMP_C", "RUNNING_MAX_HUMIDITY_VAL"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
