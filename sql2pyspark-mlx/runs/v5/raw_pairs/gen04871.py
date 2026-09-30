from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "burst_id",
        F.when(F.col("device_code").rlike("^(DV-03|DV-02)$"), "OTHER_ZONE").when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "PRIORITY").otherwise("GROUP_B").alias("device_code_zone"),
    )
)
