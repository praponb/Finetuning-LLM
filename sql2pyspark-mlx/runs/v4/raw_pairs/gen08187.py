from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("temp_c").desc(), "id")
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w_hist = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

with_windows = (
    df_device_readings
    .select(
        "*",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.max("humidity").over(w2).alias("humidity_max_15m"),
        F.ntile(5).over(w3).alias("humidity_bucket"),
        F.sum("pressure").over(w_hist).alias("running_pressure"),
        F.count("temp_c").over(w_hist_2).alias("temp_c_count_1h"),
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
        "status_bits",
        "firmware",
        "seq_no",
    )
    .orderBy(F.col("device_code").desc())
)
