from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits_times_temp_c", "id")
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("pressure_bucket").asc_nulls_first(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") * F.col("temp_c")).alias("status_bits_times_temp_c"),
    )
)

metrics = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("prev_pressure", F.lag("pressure", 1).over(w2))
    .withColumn("pressure_bucket", F.ntile(4).over(w3))
    .withColumn("humidity_count_2h", F.count("humidity").over(w4))
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy("firmware")
    .pivot("device_code", ["DV-04", "DV-03"])
    .agg(F.avg("pressure").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "firmware",
        F.col("DV-04_value_sum").alias("dv_04_value_sum"),
        F.col("DV-04_n").alias("dv_04_n"),
        F.col("DV-03_value_sum").alias("dv_03_value_sum"),
        F.col("DV-03_n").alias("dv_03_n"),
    )
)
