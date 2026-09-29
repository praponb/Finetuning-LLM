from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

metrics = (
    df_device_readings
    .filter(F.col("temp_c") >= 23)
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.min("temp_c").over(w).alias("running_min_temp_c"),
        F.count("temp_c").over(w).alias("running_count_temp_c"),
        F.max("temp_c").over(w).alias("running_max_temp_c"),
        F.sum("temp_c").over(w).alias("running_sum_temp_c"),
        F.sum("pressure").over(w2).alias("pressure_sum_1h"),
    )
)

result = (
    metrics
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "running_count_temp_c",
        "running_max_temp_c",
        "running_sum_temp_c",
    )
    .orderBy(F.col("humidity").desc())
)
