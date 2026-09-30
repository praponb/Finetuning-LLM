from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")

result = (
    df_device_readings
    .select(
        "*",
        F.ntile(10).over(w).alias("humidity_bucket"),
        F.ntile(5).over(w2).alias("temp_c_bucket"),
    )
)
