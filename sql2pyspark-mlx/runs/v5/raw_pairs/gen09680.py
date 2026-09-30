from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

metrics = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        F.count("temp_c").over(w).alias("last3_count_temp_c"),
        F.max("temp_c").over(w).alias("last3_max_temp_c"),
        F.avg("temp_c").over(w).alias("last3_avg_temp_c"),
        F.count("status_bits").over(w2).alias("status_bits_count_30m"),
    )
)

result = (
    metrics
    .select(
        "id",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        "last3_max_temp_c",
        "last3_avg_temp_c",
        "status_bits_count_30m",
    )
    .orderBy(
        F.col("last3_max_temp_c").asc_nulls_last(),
        F.col("status_bits_count_30m").desc(),
    )
)
