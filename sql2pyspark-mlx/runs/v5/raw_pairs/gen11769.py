from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        F.when(F.col("device_code").rlike("^(DV-04|DV-01)$"), "EXTENDED").when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("device_code_zone"),
    )
)
