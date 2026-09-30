from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("humidity").between(36, 45))
    .withColumn("humidity_int", F.col("humidity").cast("int"))
)

result = (
    valid
    .select("id", "device_code", "pressure", "temp_c", "humidity")
    .unpivot(
        ["id", "device_code"],
        ["pressure", "temp_c", "humidity"],
        "measure",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
