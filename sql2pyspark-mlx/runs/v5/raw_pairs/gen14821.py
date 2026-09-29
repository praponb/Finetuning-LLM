from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits_rounded", "id")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("device_code").orderBy("status_bits_rounded_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("pressure_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("device_code_sum_humidity", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.to_date("reading_ts") < "2024-01-08"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round("status_bits", 2).alias("status_bits_rounded"),
    )
)

metrics = (
    filtered
    .filter(F.col("humidity") > 44)
    .select(
        "id",
        "device_code",
        "status_bits",
        "firmware",
        F.ntile(3).over(w).alias("status_bits_rounded_bucket"),
        F.avg("pressure").over(w2).alias("pressure_avg_2"),
        F.row_number().over(w3).alias("event_seq"),
        F.sum("humidity").over(w4).alias("device_code_sum_humidity"),
        F.avg("status_bits").over(w5).alias("status_bits_avg_6_prior"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "*",
        F.first("pressure_avg_2", ignorenulls=True).over(w6).alias("first_known_pressure_avg_2"),
        F.sum("status_bits").over(w7).alias("running_status_bits"),
        F.lag("status_bits_rounded_bucket", 1).over(w8).alias("prev_status_bits_rounded_bucket"),
    )
)

base = (
    metrics_device_readings
    .filter(F.col("device_code_sum_humidity").isNotNull())
    .withColumn("event_seq_per_status_bits_avg_6_prior", (F.col("event_seq") / F.nullif("status_bits_avg_6_prior", F.lit(0))))
)

grouped = (
    base
    .groupBy(
        F.when(F.col("pressure_avg_2") >= 1024, "LARGE").otherwise("SMALL").alias("pressure_avg_2_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.when(F.col("device_code") == "DV-01", F.col("event_seq_per_status_bits_avg_6_prior"))).alias("dv_01_avg_event_seq_per_status_bits_avg_6_prior"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
    )
    .filter(F.col("dv_01_avg_event_seq_per_status_bits_avg_6_prior") < 1000000)
)

result = grouped.select("pressure_avg_2_tier", "dv_02_rows")
