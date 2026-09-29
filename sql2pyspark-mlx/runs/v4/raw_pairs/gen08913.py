from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc())
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_last(), "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("dv_04_count").desc())

ranked = (
    df_device_readings
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    ranked
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        F.col("pressure").cast("int").alias("pressure_int"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

daily = (
    deduped
    .groupBy("device_code", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.min("reading_ts").alias("first_reading_ts"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.avg(F.when(F.col("device_code") == "DV-03", F.col("pressure_int"))).alias("dv_03_avg_pressure_int"),
    )
    .withColumn("dv_04_count_dense_rank", F.dense_rank().over(w3))
)

result = (
    daily
    .select(
        "device_code",
        "hour_window",
        "first_reading_ts",
        "dv_03_avg_pressure_int",
        "dv_04_count_dense_rank",
    )
)
