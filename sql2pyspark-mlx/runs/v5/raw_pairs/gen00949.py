from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("firmware") != "2.0.1")
    .select(
        "*",
        F.when(F.col("status_bits") >= 10, "High").when(F.col("status_bits") >= 3, "Medium").otherwise("Low").alias("status_bits_tier"),
    )
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .cube(
        F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL").alias("session_id_tier"),
        F.when(F.col("humidity") >= 80, "HIGH").otherwise("LOW").alias("humidity_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("humidity")).otherwise(0)).alias("dv_01_humidity"),
        F.avg(F.when(F.col("device_code") == "DV-01", F.col("status_bits"))).alias("dv_01_avg_status_bits"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
    )
    .orderBy("humidity_tier")
)
