from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("pressure") > 1015)
    .select("*", F.least("temp_c", "humidity").alias("least_temp_c_humidity"))
)

result = (
    valid
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-04"])
    .agg(F.sum("temp_c").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "firmware",
        F.col("DV-03_amount").alias("dv_03_amount"),
        F.col("DV-03_n").alias("dv_03_n"),
        F.col("DV-04_amount").alias("dv_04_amount"),
        F.col("DV-04_n").alias("dv_04_n"),
    )
)
