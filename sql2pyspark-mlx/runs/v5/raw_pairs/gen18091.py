from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last())
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        F.rank().over(w).alias("pressure_rank"),
        F.sum("pressure").over(w2).alias("cumulative_pressure"),
    )
)
