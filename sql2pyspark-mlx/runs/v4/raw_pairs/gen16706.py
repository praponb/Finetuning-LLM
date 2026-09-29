from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("status_bits_minus_pressure").desc())
w5 = Window.partitionBy("device_code").orderBy("humidity", "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.col("reading_ts") >= "2024-12-01"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") - F.col("pressure")).alias("status_bits_minus_pressure"),
    )
)

scored = (
    valid
    .withColumn("humidity_bucket", F.ntile(3).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("corr_status_bits_temp_c", F.corr("status_bits", "temp_c").over(w3))
)

valid_device_readings = (
    scored
    .filter(F.col("pressure") <= 1008)
    .select(
        "*",
        F.greatest("status_bits_minus_pressure", "pressure").alias("greatest_status_bits_minus_pressure_pressure"),
    )
)

ranked = (
    valid_device_readings
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

ranked_device_readings = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked_device_readings
    .cube("device_code", "seq_no")
    .agg(
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("pressure")).otherwise(0)).alias("dv_01_pressure"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.percentile_approx("humidity_bucket", 0.9).alias("humidity_bucket_p90"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .orderBy(F.col("dv_01_pressure").asc_nulls_last())
)
