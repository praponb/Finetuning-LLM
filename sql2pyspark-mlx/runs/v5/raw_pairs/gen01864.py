from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("device_code").isin(["DV-03", "DV-02"]))
        .select("firmware", "device_code")
    )
)
