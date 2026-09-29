from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")

metrics = (
    df_device_readings
    .select(
        "*",
        F.ntile(4).over(w).alias("humidity_bucket"),
        F.first("humidity", ignorenulls=True).over(w2).alias("first_known_humidity"),
        F.corr("status_bits", "temp_c").over(w3).alias("corr_status_bits_temp_c"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "humidity_bucket",
        "corr_status_bits_temp_c",
        F.when(F.col("device_code").rlike("^(DV-04|DV-02)$"), "GROUP_A").otherwise("PRIORITY").alias("device_code_zone"),
    )
    .orderBy(F.col("device_code").desc_nulls_first())
)
