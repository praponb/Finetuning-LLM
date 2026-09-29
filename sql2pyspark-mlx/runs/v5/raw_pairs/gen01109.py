from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.avg("humidity").over(w).alias("humidity_avg_2"),
        F.last("humidity", ignorenulls=True).over(w2).alias("last_known_humidity"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity_avg_2",
        F.when(F.col("device_code").rlike("^(DV-04|DV-03)$"), "CORE").when(F.col("device_code").rlike("^(DV-01|DV-02)$"), "GROUP_A").otherwise("EXTENDED").alias("device_code_zone"),
    )
)
