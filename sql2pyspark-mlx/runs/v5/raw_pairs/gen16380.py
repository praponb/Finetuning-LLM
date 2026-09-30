from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("temp_c").asc_nulls_last())
w7 = Window.orderBy("temp_c")
w8 = Window.orderBy(F.col("pressure").desc_nulls_last())
w9 = Window.orderBy(F.col("temp_c").desc_nulls_last())
w10 = Window.orderBy(F.col("humidity").desc())

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select("*", F.col("pressure").cast("int").alias("pressure_int"))
)

with_windows = (
    base
    .withColumn("pressure_avg_2_prior", F.avg("pressure").over(w_ordered))
    .withColumn("corr_status_bits_pressure", F.corr("status_bits", "pressure").over(w_grp))
    .withColumn("pressure_avg_30m", F.avg("pressure").over(w3))
)

with_windows_device_readings = (
    with_windows
    .filter(F.col("corr_status_bits_pressure") >= 0.2)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "pressure_avg_2_prior",
        "pressure_avg_30m",
        F.sum("pressure_avg_30m").over(w4).alias("cumulative_pressure_avg_30m"),
        F.sum("pressure").over(w5).alias("running_pressure"),
    )
)

cleaned = (
    with_windows_device_readings
    .filter(F.col("humidity") >= 37)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "pressure_avg_2_prior",
        "pressure_avg_30m",
        "cumulative_pressure_avg_30m",
        "running_pressure",
        F.when(F.col("temp_c") >= 39.6, "High").when(F.col("temp_c") >= 25.3, "Medium").otherwise("Low").alias("temp_c_level"),
        F.round((F.col("pressure") * 1.07), 2).alias("adjusted_pressure"),
    )
)

kept_keys = (
    cleaned
    .select("id")
    .intersect(
        df_device_readings
        .filter(F.col("pressure") >= 1017)
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "temp_c",
        "humidity",
        "pressure_avg_2_prior",
        "pressure_avg_30m",
        "cumulative_pressure_avg_30m",
        "running_pressure",
        "temp_c_level",
        "adjusted_pressure",
        F.dense_rank().over(w6).alias("temp_c_dense_rank"),
        F.percent_rank().over(w7).alias("temp_c_percent_rank"),
        F.cume_dist().over(w8).alias("pressure_cume_dist"),
    )
)

enriched_device_readings = (
    enriched
    .withColumn("temp_c_percent_rank_2", F.percent_rank().over(w9))
    .withColumn("humidity_dense_rank", F.dense_rank().over(w10))
)

result = (
    enriched_device_readings
    .select(
        "temp_c",
        "pressure_avg_2_prior",
        "pressure_avg_30m",
        "cumulative_pressure_avg_30m",
        "running_pressure",
        "temp_c_level",
        "adjusted_pressure",
        "temp_c_dense_rank",
        "temp_c_percent_rank",
    )
    .orderBy(F.col("temp_c_percent_rank").desc())
)
