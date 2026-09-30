from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")

result = (
    df_device_readings
    .select(
        "*",
        F.first("humidity", ignorenulls=True).over(w).alias("first_known_humidity"),
        F.last("temp_c", ignorenulls=True).over(w2).alias("last_known_temp_c"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
