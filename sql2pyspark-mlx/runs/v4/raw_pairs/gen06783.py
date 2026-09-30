from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_first(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

filtered = (
    ranked
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("firmware").isin(["1.0.3", "1.1.0"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("status_bits") / F.col("temp_c")), 2).alias("status_bits_per_temp_c"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w3))

by_type = (
    islands
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-04", "DV-02"])
    .agg(F.avg("status_bits"))
    .select(
        "firmware",
        F.col("DV-03").alias("dv_03"),
        F.col("DV-04").alias("dv_04"),
        F.col("DV-02").alias("dv_02"),
    )
)

result = (
    by_type
    .select("firmware", "dv_03")
    .orderBy(F.col("dv_03").asc_nulls_first())
)
