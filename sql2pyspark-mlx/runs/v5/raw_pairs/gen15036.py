from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w4 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        F.avg("humidity").over(w).alias("humidity_avg_1h"),
        F.count("pressure").over(w2).alias("device_code_count_pressure"),
        F.row_number().over(w3).alias("seq_no"),
        F.sum("temp_c").over(w4).alias("running_temp_c"),
    )
)

result = (
    enriched
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "humidity_avg_1h",
        "device_code_count_pressure",
        "seq_no",
    )
    .orderBy(F.col("humidity_avg_1h").desc_nulls_last())
)
