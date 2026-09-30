from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("reading_ts_weekofyear", "id")
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("prev_pressure").desc_nulls_last(), "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.weekofyear("reading_ts").alias("reading_ts_weekofyear"),
        F.round((F.col("pressure") * 1.1), 2).alias("adjusted_pressure"),
    )
)

with_windows = (
    base
    .withColumn("running_pressure", F.sum("pressure").over(w))
    .withColumn("reading_ts_weekofyear_bucket", F.ntile(4).over(w2))
    .withColumn("prev_pressure", F.lag("pressure", 1).over(w3))
    .withColumn("humidity_bucket", F.ntile(3).over(w4))
    .withColumn("next_humidity", F.lead("humidity", 1).over(w5))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-02", "DV-01"])
    .agg(F.avg("temp_c").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "firmware",
        F.col("DV-03_value_sum").alias("dv_03_value_sum"),
        F.col("DV-03_n").alias("dv_03_n"),
        F.col("DV-02_value_sum").alias("dv_02_value_sum"),
        F.col("DV-02_n").alias("dv_02_n"),
        F.col("DV-01_value_sum").alias("dv_01_value_sum"),
        F.col("DV-01_n").alias("dv_01_n"),
    )
)
