from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last())
w3 = Window.orderBy("temp_c", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp")) & F.col("pressure").between(998, 1011))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        (F.col("humidity") * F.col("temp_c")).alias("humidity_times_temp_c"),
        (F.col("status_bits") + F.col("humidity")).alias("status_bits_plus_humidity"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .withColumn("temp_c_rank", F.rank().over(w2))
    .withColumn("humidity_times_temp_c_avg_3", F.avg("humidity_times_temp_c").over(w3))
)

filtered = (
    enriched
    .filter(F.col("rn") != 1)
    .withColumn("humidity_times_temp_c_minus_temp_c", (F.col("humidity_times_temp_c") - F.col("temp_c")))
    .withColumn("adjusted_humidity_times_temp_c_avg_3", F.round((F.col("humidity_times_temp_c_avg_3") * 100), 2))
)

daily = (
    filtered
    .groupBy(
        "device_code",
        F.when(F.col("status_bits_plus_humidity") >= 77, "PREMIUM").otherwise("STANDARD").alias("status_bits_plus_humidity_tier"),
    )
    .agg(
        F.avg(F.when(F.col("device_code") == "DV-03", F.col("adjusted_humidity_times_temp_c_avg_3"))).alias("dv_03_avg_adjusted_humidity_times_temp_c_avg_3"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("humidity_times_temp_c_minus_temp_c")).otherwise(0)).alias("dv_04_humidity_times_temp_c_minus_temp_c"),
    )
    .filter(F.col("dv_04_humidity_times_temp_c_minus_temp_c") > 0)
)

result = (
    daily
    .select(
        "device_code",
        "status_bits_plus_humidity_tier",
        "dv_03_avg_adjusted_humidity_times_temp_c_avg_3",
        "dv_03_events",
        "dv_04_humidity_times_temp_c_minus_temp_c",
    )
)
