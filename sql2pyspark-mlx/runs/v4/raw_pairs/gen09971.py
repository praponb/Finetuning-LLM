from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.orderBy("status_bits", "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 14:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        F.floor("temp_c").alias("temp_c_whole"),
    )
)

valid = (
    filtered
    .filter((F.col("reading_ts") > F.lit("2025-09-01 09:00:00").cast("timestamp")) & F.hour("reading_ts").between(7, 11))
    .select(
        "*",
        F.length("device_code").alias("device_code_length"),
        F.lower("firmware").alias("firmware_lower"),
    )
)

metrics = (
    valid
    .withColumn("prev_device_code_length", F.lag("device_code_length", 1).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("temp_c_whole_avg_2h", F.avg("temp_c_whole").over(w3))
)

result = (
    metrics
    .select(
        "id",
        "reading_ts",
        "status_bits",
        "firmware",
        "device_code_length",
        "firmware_lower",
        "prev_device_code_length",
        "seq_no",
    )
    .orderBy(F.col("device_code_length").desc_nulls_last())
)
