from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w3 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("running_min_humidity").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("running_max_humidity", "id")
w6 = Window.partitionBy("device_code").orderBy("dedup_rank", "id")
w7 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w8 = Window.partitionBy("device_code").orderBy(F.col("next_running_avg_humidity").desc(), "id")

with_windows = (
    df_device_readings
    .filter(F.col("temp_c") >= 38)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        F.max("humidity").over(w).alias("running_max_humidity"),
        F.avg("humidity").over(w).alias("running_avg_humidity"),
        F.min("humidity").over(w).alias("running_min_humidity"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "running_max_humidity",
        "running_avg_humidity",
        "running_min_humidity",
        F.last("running_min_humidity", ignorenulls=True).over(w3).alias("last_known_running_min_humidity"),
        F.ntile(10).over(w4).alias("running_min_humidity_bucket"),
        F.lead("running_avg_humidity", 1).over(w5).alias("next_running_avg_humidity"),
        F.lead("running_avg_humidity", 1, 0).over(w6).alias("next_running_avg_humidity_2"),
        F.ntile(3).over(w7).alias("status_bits_bucket"),
    )
)

latest_device_readings = (
    scored
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_device_readings
    .select(
        "id",
        "device_code",
        "next_running_avg_humidity_2",
        "running_avg_humidity",
        "next_running_avg_humidity",
        "running_max_humidity",
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["next_running_avg_humidity_2", "running_avg_humidity", "next_running_avg_humidity", "running_max_humidity"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
