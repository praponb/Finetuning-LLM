from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_last(), "id")

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .withColumn("temp_c_tier", F.when(F.col("temp_c") >= 33.7, "HIGH").otherwise("LOW"))
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 75, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["device_code", "temp_c_tier", "humidity_tier"], ["device_code", "temp_c_tier"], ["device_code"]],
        "device_code",
        "temp_c_tier",
        "humidity_tier",
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.grouping_id().alias("gid"),
    )
)
