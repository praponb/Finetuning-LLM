from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "burst_id",
        F.when(F.col("device_code").rlike("^(DV-01|DV-02)$"), "PRIORITY").when(F.col("device_code").rlike("^(DV-03|DV-04)$"), "EXTENDED").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy("id", "temp_c")
    .limit(10)
)
