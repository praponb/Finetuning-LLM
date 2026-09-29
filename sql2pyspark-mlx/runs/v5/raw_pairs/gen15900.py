from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .select(
        "*",
        F.min("pressure").over(w).alias("running_min_pressure"),
        F.max("pressure").over(w).alias("running_max_pressure"),
        F.sum("pressure").over(w).alias("running_sum_pressure"),
        F.avg("pressure").over(w).alias("running_avg_pressure"),
    )
)

result = (
    enriched
    .select(
        "id",
        "reading_ts",
        "status_bits",
        "firmware",
        "running_min_pressure",
        "running_max_pressure",
        "running_sum_pressure",
        "running_avg_pressure",
    )
)
