from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy("pressure")
w5 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("device_code").orderBy("temp_c_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy("humidity_filled", "id").rowsBetween(-2, -1)
w_hist = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy("humidity_filled_max_1h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 14:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("temp_c_percent_rank"),
        F.max("humidity_filled").over(w2).alias("humidity_filled_max_1h"),
        F.corr("pressure", "humidity").over(w3).alias("corr_pressure_humidity"),
        F.dense_rank().over(w4).alias("pressure_dense_rank"),
    )
)

enriched_device_readings = (
    enriched
    .select(
        "*",
        F.avg("temp_c").over(w5).alias("temp_c_avg_5_prior"),
        F.first("humidity_filled_max_1h", ignorenulls=True).over(w6).alias("first_known_humidity_filled_max_1h"),
        F.count("temp_c_percent_rank").over(w7).alias("temp_c_percent_rank_count_2h"),
        F.avg("humidity_filled_max_1h").over(w8).alias("humidity_filled_max_1h_avg_15m"),
        F.avg("humidity").over(w9).alias("humidity_avg_2_prior"),
    )
)

enriched_device_readings_2 = (
    enriched_device_readings
    .select(
        "*",
        F.sum("temp_c_avg_5_prior").over(w_hist).alias("running_temp_c_avg_5_prior"),
        F.first("temp_c_percent_rank_count_2h", ignorenulls=True).over(w11).alias("first_known_temp_c_percent_rank_count_2h"),
    )
)

result = (
    enriched_device_readings_2
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1031, "HIGH").otherwise("LOW"))
    .withColumn("hour_window", F.date_trunc("hour", "reading_ts"))
    .cube("pressure_tier", "hour_window")
    .agg(
        F.corr("pressure", "temp_c").alias("corr_pressure_temp_c"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("humidity_avg_2_prior", [0.25, 0.5, 0.75]).alias("humidity_avg_2_prior_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("num_rows") < 1)
    .orderBy(F.col("num_rows").desc_nulls_last())
)
