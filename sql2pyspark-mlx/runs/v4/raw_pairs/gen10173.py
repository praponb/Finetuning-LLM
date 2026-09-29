from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        (F.col("pressure") + F.col("humidity")).alias("pressure_plus_humidity"),
    )
)

flagged = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

filtered = (
    sessionized
    .filter(F.hour("reading_ts").between(11, 15))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "new_group_flag",
        F.upper("device_code").alias("device_code_upper"),
    )
)

cleaned = (
    filtered
    .filter((F.col("reading_ts") > F.lit("2025-09-01 11:00:00").cast("timestamp")) & (F.col("pressure") <= 994))
    .withColumn("status_bits_rounded", F.round("status_bits", 1))
    .withColumn("reading_ts_label", F.date_format("reading_ts", "MMM yyyy"))
)

grouped = (
    cleaned
    .groupBy("device_code")
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("temp_c")).otherwise(0)).alias("dv_02_temp_c"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
)

result = (
    grouped
    .select(
        "device_code",
        "dv_02_temp_c",
        "new_group_flag_quartiles",
        "num_device_codes",
    )
    .orderBy(F.col("dv_02_temp_c").desc_nulls_first(), "num_device_codes")
)
