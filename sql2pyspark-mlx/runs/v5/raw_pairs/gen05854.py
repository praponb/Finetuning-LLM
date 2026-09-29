from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_device_readings
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 10, "HIGH").otherwise("LOW"))
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1018, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["status_bits_tier", "pressure_tier"], ["status_bits_tier"], []],
        "status_bits_tier",
        "pressure_tier",
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
    )
    .orderBy(F.col("status_bits_tier").desc())
)
