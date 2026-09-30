from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("status_bits") >= 12, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
    )
    .agg(
        F.min("reading_ts").alias("first_seen"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("session_id_per_dv_02"),
    )
)
