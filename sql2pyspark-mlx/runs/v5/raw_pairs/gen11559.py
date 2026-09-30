from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("is_2_0_1", "id")
w2 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("firmware") == "2.0.1", 1).otherwise(0).alias("is_2_0_1"),
    )
)

result = (
    base
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        "is_2_0_1",
        F.lead("pressure").over(w_running).alias("next_pressure"),
        F.lag("status_bits").over(w_running).alias("prev_status_bits"),
        F.lead("status_bits").over(w_running).alias("next_status_bits"),
        F.row_number().over(w_running).alias("event_seq"),
        F.last("humidity", ignorenulls=True).over(w2).alias("last_known_humidity"),
    )
)
