from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("pressure").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
        (F.col("humidity") * F.col("temp_c")).alias("humidity_times_temp_c"),
    )
)

result = (
    filtered
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "days_since_reading_ts",
        "humidity_times_temp_c",
        F.max("humidity").over(w).alias("last3_max_humidity"),
        F.min("humidity").over(w).alias("last3_min_humidity"),
        F.avg("humidity").over(w).alias("last3_avg_humidity"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
