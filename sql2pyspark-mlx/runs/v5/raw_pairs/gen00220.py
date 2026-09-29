from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("adjusted_pressure").asc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("pressure", "id")
w6 = Window.partitionBy("device_code").orderBy("adjusted_pressure_percent_rank", "id")
w7 = Window.orderBy(F.col("pressure").desc_nulls_first())
w_recent = Window.orderBy("adjusted_pressure", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("humidity") >= 63))
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("pressure") * 100), 2).alias("adjusted_pressure"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w).alias("adjusted_pressure_percent_rank"),
        F.row_number().over(w2).alias("row_seq"),
        F.rank().over(w3).alias("status_bits_rank"),
        F.first("adjusted_pressure", ignorenulls=True).over(w4).alias("first_known_adjusted_pressure"),
        F.lead("adjusted_pressure", 2).over(w5).alias("next_adjusted_pressure"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "firmware",
        "adjusted_pressure",
        "status_bits_rank",
        "next_adjusted_pressure",
        F.lag("row_seq").over(w6).alias("prev_row_seq"),
        F.lead("humidity").over(w6).alias("next_humidity"),
        F.row_number().over(w6).alias("row_seq_2"),
        F.rank().over(w7).alias("pressure_rank"),
    )
)

metrics_device_readings = (
    with_windows
    .select(
        "id",
        "firmware",
        "adjusted_pressure",
        "status_bits_rank",
        "next_adjusted_pressure",
        "next_humidity",
        "row_seq_2",
        F.lag("pressure_rank").over(w_recent).alias("prev_pressure_rank"),
        F.lag("next_adjusted_pressure").over(w_recent).alias("prev_next_adjusted_pressure"),
        F.lead("next_adjusted_pressure").over(w_recent).alias("next_next_adjusted_pressure"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

result = (
    metrics_device_readings
    .select(
        "id",
        "firmware",
        "adjusted_pressure",
        "status_bits_rank",
        "next_adjusted_pressure",
        "row_seq_2",
        "prev_pressure_rank",
        "prev_next_adjusted_pressure",
        "event_seq",
    )
    .orderBy(F.col("row_seq_2").desc(), "adjusted_pressure")
)
