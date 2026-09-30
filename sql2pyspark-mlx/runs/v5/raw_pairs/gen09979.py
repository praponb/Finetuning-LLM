from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("firmware", "id")
    .intersect(
        df_device_readings
        .filter(F.col("pressure").between(1008, 1016))
        .select("firmware", "id")
    )
)
