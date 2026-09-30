from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("device_code").orderBy("status_bits_avg_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("humidity", "id")
w9 = Window.partitionBy("device_code").orderBy("running_seq_no", "id")
w10 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

scored = (
    df_device_readings
    .select(
        "*",
        F.round((F.col("temp_c") / F.sum("temp_c").over(w)), 4).alias("temp_c_share"),
        F.min("pressure").over(w).alias("device_code_min_pressure"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_30m"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .withColumn("pressure_avg_2_prior", F.avg("pressure").over(w5))
    .withColumn("cumulative_pressure", F.sum("pressure").over(w6))
    .withColumn("running_seq_no", F.sum("seq_no").over(w7))
    .withColumn("event_seq", F.row_number().over(w8))
    .withColumn("device_code_avg_status_bits_avg_30m", F.avg("status_bits_avg_30m").over(w))
)

metrics = (
    enriched
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "temp_c_share",
        "device_code_min_pressure",
        "cumulative_pressure",
        F.lag("pressure", 1).over(w9).alias("prev_pressure"),
        F.count("pressure").over(w10).alias("pressure_count_2h"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.max("reading_ts").alias("ended_at"),
        F.percentile_approx("cumulative_pressure", 0.5).alias("cumulative_pressure_p50"),
        F.min("reading_ts").alias("first_reading_ts"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cumulative_pressure_p50").desc())
)
