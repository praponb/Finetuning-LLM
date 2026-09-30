from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")

scored = (
    df_device_readings
    .select(
        "id",
        "temp_c",
        "pressure",
        "firmware",
        F.avg("temp_c").over(w).alias("temp_c_avg_2"),
        F.last("temp_c", ignorenulls=True).over(w2).alias("last_known_temp_c"),
        F.corr("temp_c", "humidity").over(w3).alias("corr_temp_c_humidity"),
    )
)

result = (
    scored
    .select("id", "temp_c", "pressure", "temp_c_avg_2", "corr_temp_c_humidity")
    .orderBy(F.col("temp_c_avg_2").desc_nulls_first())
)
