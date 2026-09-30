from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("humidity") >= 61)
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware",
        (F.col("temp_c") - F.col("status_bits")).alias("temp_c_minus_status_bits"),
    )
)

result = (
    base
    .select("id", "device_code", "pressure", "temp_c_minus_status_bits")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "temp_c_minus_status_bits"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
