from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("burst_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("burst_id_per_dv_03"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
    )
    .orderBy(F.col("avg_status_bits").asc_nulls_last())
)
