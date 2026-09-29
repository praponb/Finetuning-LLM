from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("temp_c")
w2 = Window.orderBy("reading_ts", "id")
w_recent = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("pressure").asc_nulls_last())
w5 = Window.orderBy(F.col("reading_ts_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy("reading_ts_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("pressure") <= 1023))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.round("pressure", 1).alias("pressure_rounded"),
    )
)

filtered = (
    valid
    .filter(F.to_date("reading_ts") < "2024-12-01")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "pressure_rounded",
        (F.col("humidity") - F.col("pressure_rounded")).alias("humidity_minus_pressure_rounded"),
    )
)

base = (
    filtered
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "pressure_rounded",
        F.date_trunc("week", "reading_ts").alias("reading_ts_period"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "reading_ts_period",
        F.percent_rank().over(w).alias("temp_c_percent_rank"),
        F.lead("humidity", 2).over(w2).alias("next_humidity"),
        F.first("temp_c", ignorenulls=True).over(w_recent).alias("first_known_temp_c"),
        F.percent_rank().over(w4).alias("pressure_percent_rank"),
        F.first("humidity", ignorenulls=True).over(w_recent).alias("first_known_humidity"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "reading_ts_period",
        "first_known_temp_c",
        "first_known_humidity",
        F.max("pressure_percent_rank").over(w5).alias("pressure_percent_rank_max_7d"),
        F.last("temp_c_percent_rank", ignorenulls=True).over(w6).alias("last_known_temp_c_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("temp_c") >= 38.5, "HIGH").otherwise("LOW").alias("temp_c_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts_period")) - F.unix_timestamp(F.min("reading_ts_period"))) / 60).alias("span_minutes"),
        F.min("reading_ts").alias("started_at"),
        F.min("reading_ts_period").alias("started_at_2"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
