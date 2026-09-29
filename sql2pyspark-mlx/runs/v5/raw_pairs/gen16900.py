from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w3 = Window.orderBy("least_pressure_humidity", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity_percent_rank").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("seq_no", "id")
w_running = Window.partitionBy("device_code").orderBy("next_pressure", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 05:00:00"))) & F.col("device_code").isin(["DV-02", "DV-01", "DV-04"]))
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "firmware",
        F.least("pressure", "humidity").alias("least_pressure_humidity"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w).alias("humidity_percent_rank"),
        F.lead("pressure", 1).over(w2).alias("next_pressure"),
        F.lead("humidity", 2).over(w2).alias("next_humidity"),
        F.avg("pressure").over(w3).alias("pressure_avg_6"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.row_number().over(w4).alias("seq_no"),
        F.first("humidity_percent_rank", ignorenulls=True).over(w5).alias("first_known_humidity_percent_rank"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

with_windows_device_readings = (
    latest
    .withColumn("next_seq_no", F.lead("seq_no").over(w_running))
    .withColumn("next_pressure_2", F.lead("pressure").over(w_running))
    .withColumn("prev_pressure", F.lag("pressure").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

latest_device_readings = (
    with_windows_device_readings
    .withColumn("dedup_rank_2", F.row_number().over(w4))
    .filter(F.col("dedup_rank_2") == 1)
)

result = (
    latest_device_readings
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware",
        "least_pressure_humidity",
        "humidity_percent_rank",
        "next_pressure",
        "next_humidity",
        "pressure_avg_6",
    )
    .orderBy(F.col("next_humidity").desc(), "next_pressure")
)
