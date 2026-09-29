from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "island_id",
        F.when(F.col("device_code").rlike("^(DV-04|DV-03)$"), "OTHER_ZONE").when(F.col("device_code").rlike("^(DV-01|DV-02)$"), "GROUP_A").otherwise("CORE").alias("device_code_zone"),
    )
    .orderBy(F.col("humidity").desc())
)
