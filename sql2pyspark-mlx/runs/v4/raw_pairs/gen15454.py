from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first())
w2 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_device_readings
    .select(
        "*",
        F.dense_rank().over(w).alias("pressure_dense_rank"),
        F.first("temp_c", ignorenulls=True).over(w2).alias("first_known_temp_c"),
        F.first("temp_c", ignorenulls=True).over(w3).alias("first_known_temp_c_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
    )
    .orderBy(F.col("reading_ts").desc_nulls_first())
)
