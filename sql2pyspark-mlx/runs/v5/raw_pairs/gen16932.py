from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

valid = (
    latest
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 06:00:00")))
    .select(
        "*",
        F.initcap(F.trim("firmware")).alias("firmware_title"),
        F.floor("humidity").alias("humidity_whole"),
    )
)

valid_device_readings = (
    valid
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.to_date("reading_ts") < "2025-07-01"))
    .withColumn("reading_ts_day", F.col("reading_ts").cast("date"))
)

deduped = (
    valid_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

gaps = (
    deduped
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w3))

result = (
    sessionized
    .groupBy(
        F.when(F.col("burst_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.corr("pressure", "temp_c").alias("corr_pressure_temp_c"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
)
