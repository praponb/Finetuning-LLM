from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("pressure_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("pressure", "id")
w3 = Window.orderBy("pressure_filled", "id")
w4 = Window.orderBy("status_bits")
w5 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("firmware") == "2.0.1")
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        F.coalesce("pressure", F.lit(0)).alias("pressure_filled"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.sum("humidity").over(w).alias("cumulative_humidity"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("pressure_filled", 1).over(w3).alias("next_pressure_filled"),
        F.dense_rank().over(w4).alias("status_bits_dense_rank"),
        F.last("pressure_filled", ignorenulls=True).over(w5).alias("last_known_pressure_filled"),
    )
)
