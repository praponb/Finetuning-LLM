from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w3 = Window.partitionBy("device_code")

scored = (
    df_device_readings
    .select(
        "*",
        F.avg("humidity").over(w).alias("humidity_avg_2"),
        F.lag("humidity", 1).over(w2).alias("prev_humidity"),
        F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4).alias("humidity_share"),
    )
)

result = (
    scored
    .select(
        "id",
        "reading_ts",
        "firmware",
        "humidity_avg_2",
        "prev_humidity",
        "humidity_share",
    )
)
