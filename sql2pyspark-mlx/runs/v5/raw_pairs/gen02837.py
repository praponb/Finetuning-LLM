from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_hist = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("pressure", "id")
w8 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("humidity") >= 73, "High").when(F.col("humidity") >= 44, "Medium").otherwise("Low").alias("humidity_level"),
    )
)

enriched = (
    base
    .filter(F.col("status_bits") == 11)
    .select(
        "*",
        F.lead("humidity", 1).over(w).alias("next_humidity"),
        F.sum("status_bits").over(w2).alias("running_status_bits"),
        F.sum("pressure").over(w3).alias("pressure_sum_15m"),
        F.corr("temp_c", "pressure").over(w_hist).alias("corr_temp_c_pressure"),
    )
)

prev_events = enriched.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        "humidity_level",
        "running_status_bits",
        F.sum("is_new_session").over(w5).alias("burst_id"),
    )
)

with_windows = (
    sessionized
    .select(
        "id",
        "device_code",
        "temp_c",
        "firmware",
        F.first("running_status_bits", ignorenulls=True).over(w_grp).alias("first_known_running_status_bits"),
        F.lead("pressure", 1).over(w7).alias("next_pressure"),
        F.first("running_status_bits", ignorenulls=True).over(w2).alias("first_known_running_status_bits_2"),
        F.last("pressure", ignorenulls=True).over(w8).alias("last_known_pressure"),
    )
)

valid = (
    with_windows
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "id",
        "device_code",
        "temp_c",
        "next_pressure",
        "last_known_pressure",
        F.col("first_known_running_status_bits_2").cast("double").alias("first_known_running_status_bits_2_num"),
    )
)

result = (
    valid
    .select(
        "id",
        "temp_c",
        "next_pressure",
        "last_known_pressure",
        "first_known_running_status_bits_2_num",
    )
    .orderBy(F.col("temp_c").desc_nulls_first(), F.col("next_pressure").desc())
)
