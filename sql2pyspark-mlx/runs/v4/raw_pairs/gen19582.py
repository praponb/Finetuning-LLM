from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy("humidity", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc())

scored = (
    df_device_readings
    .select(
        "*",
        F.max("status_bits").over(w).alias("status_bits_max_30m"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_5_prior"),
        F.rank().over(w3).alias("pressure_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("pressure") >= 1024, "PREMIUM").otherwise("STANDARD").alias("pressure_tier"),
        F.when(F.col("status_bits_avg_5_prior") >= 12, "LARGE").otherwise("SMALL").alias("status_bits_avg_5_prior_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
)
