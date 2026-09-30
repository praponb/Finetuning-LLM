from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")

scored = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-01", "DV-02", "DV-04"]))
    .select(
        "*",
        F.sum("status_bits").over(w).alias("running_status_bits"),
        F.ntile(10).over(w2).alias("status_bits_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "running_status_bits",
        "status_bits_bucket",
    )
    .orderBy(F.col("status_bits_bucket").desc_nulls_last())
)
