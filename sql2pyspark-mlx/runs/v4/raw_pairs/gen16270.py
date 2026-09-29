from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("temp_c", "id")
w_ordered = Window.orderBy("temp_c_minus_status_bits", "id")
w3 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("pressure").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 14:00:00").cast("timestamp"))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") - F.col("status_bits")).alias("temp_c_minus_status_bits"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.lead("pressure", 1).over(w).alias("next_pressure"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.first("temp_c", ignorenulls=True).over(w3).alias("first_known_temp_c"),
        F.ntile(10).over(w_part).alias("pressure_bucket"),
    )
)
