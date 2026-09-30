from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("next_humidity", "id")
w4 = Window.partitionBy("device_code").orderBy("temp_c")
w5 = Window.partitionBy("device_code").orderBy("next_pressure", "id")
w6 = Window.partitionBy("device_code").orderBy("next_next_humidity", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w8 = Window.orderBy("next_next_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device_code")
w10 = Window.partitionBy("device_code").orderBy(F.col("next_next_humidity").desc(), "id")
w11 = Window.partitionBy("device_code").orderBy("humidity_plus_pressure_avg_3_prior", "id").rowsBetween(-5, -1)
w12 = Window.orderBy("corr_temp_c_pressure", "id")
w13 = Window.orderBy("humidity_plus_pressure_avg_3_prior", "id")

enriched = (
    df_device_readings
    .select(
        "*",
        F.lead("pressure").over(w_recent).alias("next_pressure"),
        F.lead("humidity").over(w_recent).alias("next_humidity"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.avg("pressure").over(w2).alias("pressure_avg_15m"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "next_pressure",
        "next_humidity",
        "pressure_avg_15m",
        F.lead("next_humidity", 2).over(w3).alias("next_next_humidity"),
        F.percent_rank().over(w4).alias("temp_c_percent_rank"),
        F.lead("next_humidity", 1).over(w5).alias("next_next_humidity_2"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "device_code",
        "humidity",
        "next_pressure",
        "pressure_avg_15m",
        "next_next_humidity",
        "next_next_humidity_2",
        F.avg("pressure").over(w6).alias("pressure_avg_3_prior"),
        F.lag("pressure", 1).over(w7).alias("prev_pressure"),
        F.last("pressure", ignorenulls=True).over(w8).alias("last_known_pressure"),
        F.corr("temp_c", "pressure").over(w9).alias("corr_temp_c_pressure"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("next_next_humidity_2") >= 37)
    .withColumn("pressure_avg_15m_level", F.when(F.col("pressure_avg_15m") >= 1023, "High").when(F.col("pressure_avg_15m") >= 998, "Medium").otherwise("Low"))
    .withColumn("humidity_plus_pressure_avg_3_prior", (F.col("humidity") + F.col("pressure_avg_3_prior")))
)

enriched_device_readings = (
    cleaned
    .filter(F.col("pressure_avg_15m") >= 1020)
    .select(
        "id",
        "humidity",
        "next_pressure",
        "next_next_humidity_2",
        "pressure_avg_3_prior",
        "pressure_avg_15m_level",
        F.row_number().over(w10).alias("event_seq"),
        F.avg("next_next_humidity_2").over(w11).alias("next_next_humidity_2_avg_5_prior"),
        F.lag("humidity_plus_pressure_avg_3_prior", 1).over(w12).alias("prev_humidity_plus_pressure_avg_3_prior"),
        F.row_number().over(w13).alias("seq_no"),
    )
)

result = (
    enriched_device_readings
    .select(
        "id",
        "humidity",
        "next_pressure",
        "next_next_humidity_2",
        "pressure_avg_15m_level",
        "event_seq",
        "next_next_humidity_2_avg_5_prior",
        "prev_humidity_plus_pressure_avg_3_prior",
        "seq_no",
    )
    .orderBy(F.col("humidity").desc_nulls_last(), "event_seq")
)
