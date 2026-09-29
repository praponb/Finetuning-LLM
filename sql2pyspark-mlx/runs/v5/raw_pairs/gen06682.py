from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.orderBy("pressure", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "status_bits",
        "firmware",
        F.ntile(3).over(w_part).alias("humidity_bucket"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_5"),
        F.lag("status_bits", 1).over(w_part).alias("prev_status_bits"),
    )
)
