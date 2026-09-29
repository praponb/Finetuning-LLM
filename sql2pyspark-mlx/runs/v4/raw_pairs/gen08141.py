from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code")

metrics = (
    df_device_readings
    .select(
        "*",
        F.lag("temp_c", 1).over(w).alias("prev_temp_c"),
        F.lag("status_bits", 2).over(w).alias("prev_status_bits"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w2)), 4).alias("temp_c_share"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "prev_status_bits",
        "temp_c_share",
    )
    .orderBy("temp_c")
)
