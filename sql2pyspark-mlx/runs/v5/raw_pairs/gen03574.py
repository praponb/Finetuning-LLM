from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("humidity", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 09:00:00").cast("timestamp")) & (F.to_date("reading_ts") >= "2023-06-01"))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.upper("device_code").alias("device_code_upper"),
        (F.col("humidity") - F.col("temp_c")).alias("humidity_minus_temp_c"),
    )
)

result = (
    filtered
    .withColumn("running_status_bits", F.sum("status_bits").over(w))
    .withColumn("humidity_bucket", F.ntile(5).over(w2))
)
