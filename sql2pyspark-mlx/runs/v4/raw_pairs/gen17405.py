from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-04", "DV-02"])
    .agg(F.sum("temp_c").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "firmware",
        F.col("DV-03_value_sum").alias("dv_03_value_sum"),
        F.col("DV-03_item_count").alias("dv_03_item_count"),
        F.col("DV-04_value_sum").alias("dv_04_value_sum"),
        F.col("DV-04_item_count").alias("dv_04_item_count"),
        F.col("DV-02_value_sum").alias("dv_02_value_sum"),
        F.col("DV-02_item_count").alias("dv_02_item_count"),
    )
)
