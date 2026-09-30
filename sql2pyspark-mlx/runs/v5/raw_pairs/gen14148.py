from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits")
w4 = Window.orderBy(F.col("pressure_whole").desc())
w5 = Window.orderBy("pressure_whole_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.orderBy("status_bits_max_30m", "id")
w8 = Window.orderBy("status_bits_max_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 02:00:00"))) & (F.col("reading_ts") >= "2025-08-11"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.floor("pressure").alias("pressure_whole"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "pressure_whole",
        F.avg("pressure_whole").over(w).alias("pressure_whole_avg_5"),
        F.max("status_bits").over(w2).alias("status_bits_max_30m"),
        F.dense_rank().over(w3).alias("status_bits_dense_rank"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "pressure_whole",
        "status_bits_max_30m",
        F.dense_rank().over(w4).alias("pressure_whole_dense_rank"),
        F.first("pressure", ignorenulls=True).over(w5).alias("first_known_pressure"),
        F.max("pressure").over(w6).alias("pressure_max_15m"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w7).alias("status_bits_max_30m_bucket"),
        F.first("temp_c", ignorenulls=True).over(w8).alias("first_known_temp_c"),
        F.first("status_bits_max_30m", ignorenulls=True).over(w8).alias("first_known_status_bits_max_30m"),
    )
)

result = (
    metrics_device_readings
    .select("id", "temp_c", "first_known_pressure", "pressure", "first_known_temp_c")
    .unpivot(
        ids=["id"],
        values=["temp_c", "first_known_pressure", "pressure", "first_known_temp_c"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
