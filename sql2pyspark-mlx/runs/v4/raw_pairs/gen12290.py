from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

result = (
    df_device_readings
    .select(
        "*",
        F.lead("temp_c", 1, 0).over(w).alias("next_temp_c"),
        F.max("humidity").over(w2).alias("humidity_max_15m"),
    )
)
