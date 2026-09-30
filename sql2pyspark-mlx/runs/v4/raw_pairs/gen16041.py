from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first(), "id")
w_hist = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last())
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("humidity") <= 53))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        (F.col("humidity") - F.col("pressure")).alias("humidity_minus_pressure"),
    )
)

base = (
    filtered
    .filter(F.col("temp_c") < 27)
    .withColumn("temp_c_plus_humidity_minus_pressure", (F.col("temp_c") + F.col("humidity_minus_pressure")))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .withColumn("pressure_rank", F.rank().over(w_hist))
    .withColumn("running_temp_c_plus_humidity_minus_pressure", F.sum("temp_c_plus_humidity_minus_pressure").over(w3))
)

result = (
    scored
    .groupBy(
        F.when(F.col("temp_c") >= 35.6, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt_2"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.count("*").alias("cnt_3"),
    )
    .filter(F.col("cnt_3") < 1)
    .orderBy(F.col("cnt").asc_nulls_first())
)
