from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.round(F.sum("temp_c") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("temp_c_per_dv_03"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.min("reading_ts").alias("first_seen"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
)
