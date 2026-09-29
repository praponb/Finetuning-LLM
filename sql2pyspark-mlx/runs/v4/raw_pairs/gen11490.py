from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")

result = (
    df_device_readings
    .select(
        "*",
        F.round((F.col("pressure") / F.sum("pressure").over(w)), 4).alias("pressure_share"),
        F.last("pressure", ignorenulls=True).over(w2).alias("last_known_pressure"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
