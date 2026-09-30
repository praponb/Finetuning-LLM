from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits_bucket").desc_nulls_first(), "id")
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("status_bits_bucket").asc_nulls_first())
w6 = Window.partitionBy("device_code")
w7 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_device_readings
    .select(
        "*",
        F.ntile(3).over(w).alias("status_bits_bucket"),
        F.ntile(10).over(w2).alias("humidity_bucket"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .withColumn("last_known_humidity_bucket", F.last("humidity_bucket", ignorenulls=True).over(w4))
    .withColumn("status_bits_bucket_rank", F.rank().over(w_hist))
    .withColumn("temp_c_share", F.round((F.col("temp_c") / F.sum("temp_c").over(w6)), 4))
    .withColumn("humidity_bucket_2", F.ntile(3).over(w2))
    .withColumn("first_known_pressure", F.first("pressure", ignorenulls=True).over(w7))
)

result = (
    scored
    .select(
        "id",
        "device_code",
        F.col("first_known_pressure").alias("FIRST_KNOWN_PRESSURE_VAL"),
        "humidity",
        F.col("temp_c").alias("TEMP_C"),
        "pressure",
    )
    .unpivot(
        ["id", "device_code"],
        ["FIRST_KNOWN_PRESSURE_VAL", "humidity", "TEMP_C", "pressure"],
        "kpi",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
