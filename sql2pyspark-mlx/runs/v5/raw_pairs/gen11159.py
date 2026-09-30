from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select("*", F.date_format("reading_ts", "yyyy-MM").alias("reading_ts_label"))
)

result = (
    base
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-01"])
    .agg(F.sum("status_bits").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "firmware",
        F.col("DV-02_value_sum").alias("dv_02_value_sum"),
        F.col("DV-02_n").alias("dv_02_n"),
        F.col("DV-01_value_sum").alias("dv_01_value_sum"),
        F.col("DV-01_n").alias("dv_01_n"),
    )
)
