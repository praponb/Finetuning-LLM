from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("humidity") < 51)
    .select(
        "*",
        F.trim("device_code").alias("clean_device_code"),
        F.when(F.col("device_code").isin(["DV-02", "DV-04"]), "Priority").otherwise("Other").alias("device_code_group"),
    )
)

result = (
    base
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-01", "DV-04"])
    .agg(F.avg("status_bits").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "firmware",
        F.col("DV-03_total").alias("dv_03_total"),
        F.col("DV-03_n").alias("dv_03_n"),
        F.col("DV-01_total").alias("dv_01_total"),
        F.col("DV-01_n").alias("dv_01_n"),
        F.col("DV-04_total").alias("dv_04_total"),
        F.col("DV-04_n").alias("dv_04_n"),
    )
)
