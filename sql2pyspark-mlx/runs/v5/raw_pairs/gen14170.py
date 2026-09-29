from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        F.max("temp_c").over(w_recent).alias("running_max_temp_c"),
        F.avg("temp_c").over(w_recent).alias("running_avg_temp_c"),
        F.min("temp_c").over(w_recent).alias("running_min_temp_c"),
        F.count("temp_c").over(w_recent).alias("running_count_temp_c"),
    )
)
