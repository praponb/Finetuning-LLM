from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-3, -1)
w6 = Window.partitionBy("device_code").orderBy(F.col("device_code_count_status_bits").desc(), "id")
w_running = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("device_code").orderBy(F.col("pressure_bucket").desc(), "id")
w9 = Window.orderBy("status_bits", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("device_code").orderBy(F.col("device_code_count_status_bits").desc_nulls_last(), "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("status_bits") > 6))
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.substring("device_code", 1, 2).alias("device_code_prefix"),
    )
)

metrics = (
    valid
    .withColumn("cumulative_humidity", F.sum("humidity").over(w))
    .withColumn("humidity_bucket", F.ntile(5).over(w2))
    .withColumn("device_code_count_status_bits", F.count("status_bits").over(w3))
    .withColumn("pressure_bucket", F.ntile(4).over(w4))
    .withColumn("pressure_avg_3_prior", F.avg("pressure").over(w5))
)

scored = (
    metrics
    .select(
        "*",
        F.ntile(5).over(w6).alias("device_code_count_status_bits_bucket"),
        F.avg("pressure_avg_3_prior").over(w_running).alias("pressure_avg_3_prior_avg_5_prior"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

metrics_device_readings = (
    deduped
    .select(
        "*",
        F.round((F.col("pressure_avg_3_prior") / F.sum("pressure_avg_3_prior").over(w3)), 4).alias("pressure_avg_3_prior_share"),
        F.avg("pressure").over(w9).alias("pressure_avg_5"),
    )
)

ranked = (
    metrics_device_readings
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    ranked
    .filter(F.col("pressure_avg_3_prior_avg_5_prior") < 1026)
    .select("*", F.initcap(F.trim("device_code")).alias("device_code_title"))
)

wide = (
    base
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-03", "DV-04"])
    .agg(F.avg("pressure_bucket").alias("value_sum"))
    .select(
        "firmware",
        F.col("DV-01").alias("dv_01"),
        F.col("DV-03").alias("dv_03"),
        F.col("DV-04").alias("dv_04"),
    )
)

result = (
    wide
    .select("firmware", "dv_03", "dv_04")
    .orderBy(F.col("firmware").desc(), F.col("dv_03").desc())
)
