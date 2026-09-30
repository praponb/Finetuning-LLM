from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 02:00:00")))
    .select("*", F.ceil("temp_c").alias("temp_c_whole"))
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "temp_c_whole",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .withColumn("temp_c_whole_tier", F.when(F.col("temp_c_whole") >= 75, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["temp_c_whole_tier", "device_code"], ["temp_c_whole_tier"], []],
        "temp_c_whole_tier",
        "device_code",
    )
    .agg(
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.stddev_samp("humidity").alias("humidity_stddev"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("pressure_stddev").asc_nulls_last())
)
