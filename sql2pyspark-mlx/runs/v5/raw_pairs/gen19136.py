from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "session_start_flag",
        F.when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("device_code_zone"),
    )
    .orderBy(F.col("pressure").desc_nulls_last())
)
