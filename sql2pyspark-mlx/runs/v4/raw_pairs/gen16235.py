from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc())
w3 = Window.orderBy("status_bits", "id")

result = (
    df_device_readings
    .select(
        "*",
        F.sum("status_bits").over(w).alias("running_status_bits"),
        F.percent_rank().over(w2).alias("temp_c_percent_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
