from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w3 = Window.orderBy(F.col("temp_c").desc_nulls_first())
w_ordered = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "*",
        F.row_number().over(w_part).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.cume_dist().over(w3).alias("temp_c_cume_dist"),
        F.count("status_bits").over(w_ordered).alias("device_code_count_status_bits"),
        F.sum("status_bits").over(w5).alias("cumulative_status_bits"),
    )
)
