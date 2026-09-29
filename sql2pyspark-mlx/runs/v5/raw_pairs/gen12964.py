from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-3, Window.currentRow)

metrics = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.min("humidity").over(w_ordered).alias("last4_min_humidity"),
        F.avg("humidity").over(w_ordered).alias("last4_avg_humidity"),
        F.sum("humidity").over(w_ordered).alias("last4_sum_humidity"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "last4_min_humidity",
        "last4_avg_humidity",
        "last4_sum_humidity",
        F.when(F.col("device_code").rlike("^(DV-03|DV-01)$"), "GROUP_B").when(F.col("device_code").rlike("^(DV-02|DV-04)$"), "PRIORITY").otherwise("EXTENDED").alias("device_code_zone"),
    )
)
