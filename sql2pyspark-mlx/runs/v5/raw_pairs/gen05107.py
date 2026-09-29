from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity_share").desc_nulls_last())
w5 = Window.partitionBy("device_code").orderBy("humidity_share", "id")
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w7 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.hour("reading_ts").between(11, 14))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.col("humidity").cast("int").alias("humidity_int"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        F.ntile(3).over(w).alias("temp_c_bucket"),
        F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4).alias("humidity_share"),
    )
)

enriched_device_readings = (
    enriched
    .filter(F.col("device_code") == "DV-04")
    .select(
        "*",
        F.avg("pressure").over(w3).alias("pressure_avg_15m"),
        F.dense_rank().over(w4).alias("humidity_share_dense_rank"),
    )
)

deduped = (
    enriched_device_readings
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

with_prev = deduped.withColumn("prev_reading_ts", F.lag("reading_ts").over(w6))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "pressure_avg_15m",
        "latest_rank",
        F.sum("is_new_session").over(w7).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .cube(
        F.when(F.col("pressure") >= 1034, "LARGE").otherwise("SMALL").alias("pressure_tier"),
        F.date_trunc("hour", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.min("reading_ts").alias("first_reading_ts"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
    )
)
