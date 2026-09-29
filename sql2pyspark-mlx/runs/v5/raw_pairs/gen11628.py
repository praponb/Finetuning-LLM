from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("firmware_lower", F.lower("firmware"))
    .withColumn("status_bits_minus_humidity", (F.col("status_bits") - F.col("humidity")))
)

with_prev = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware_lower",
        "status_bits_minus_humidity",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

latest = (
    sessionized
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

with_gaps_2 = (
    latest
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized_device_readings = with_gaps_2.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized_device_readings
    .select(
        "id",
        "device_code",
        F.col("pressure").alias("PRESSURE_VAL"),
        F.col("temp_c").alias("TEMP_C_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["PRESSURE_VAL", "TEMP_C_VAL"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
