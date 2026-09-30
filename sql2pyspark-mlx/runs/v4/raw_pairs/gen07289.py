from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("status_bits") < 10)
    .select("*", F.col("temp_c").cast("int").alias("temp_c_int"))
)

prev_events = valid.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        "session_no",
        F.when(F.col("pressure") >= 1031, "PREMIUM").otherwise("STANDARD").alias("pressure_tier"),
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("session_no").desc())
)
