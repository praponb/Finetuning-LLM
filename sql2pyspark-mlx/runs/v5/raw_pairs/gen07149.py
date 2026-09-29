from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("pressure") > 1012)
    .select(
        "*",
        F.round((F.col("pressure") / F.col("humidity")), 2).alias("pressure_per_humidity"),
        F.when(F.col("device_code").isin(["DV-03", "DV-02"]), "Priority").otherwise("Other").alias("device_code_group"),
    )
)

prev_events = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.count("*").alias("cnt"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
