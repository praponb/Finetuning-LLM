from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        F.when(F.col("device_code").rlike("^(DV-03|DV-02)$"), "OTHER_ZONE").when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "GROUP_A").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("reading_ts").asc_nulls_first())
)
