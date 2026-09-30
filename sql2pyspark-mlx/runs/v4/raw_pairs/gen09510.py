from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("firmware").isin(["2.0.1", "1.1.0", "1.0.3"]))
    .withColumn("temp_c_filled", F.coalesce("temp_c", F.lit(0)))
)

metrics = (
    valid
    .withColumn("next_status_bits", F.lead("status_bits", 1).over(w))
    .withColumn("device_code_sum_humidity", F.sum("humidity").over(w2))
)

enriched = (
    metrics
    .select(
        "*",
        F.avg("temp_c").over(w3).alias("temp_c_avg_30m"),
        F.first("temp_c", ignorenulls=True).over(w4).alias("first_known_temp_c"),
        F.sum("status_bits").over(w4).alias("running_status_bits"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "next_status_bits",
    )
    .orderBy(F.col("firmware").desc_nulls_first())
)
