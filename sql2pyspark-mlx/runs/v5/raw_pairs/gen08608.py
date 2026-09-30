from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("row_num").desc(), "id")
w3 = Window.partitionBy("device_code")
w_recent = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("latest_rank", "id")
w6 = Window.partitionBy("device_code").orderBy("humidity", "id")
w7 = Window.partitionBy("device_code").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("last_known_humidity", "id")

latest = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.max("status_bits").over(w3).alias("device_code_max_status_bits"),
        F.last("temp_c", ignorenulls=True).over(w_recent).alias("last_known_temp_c"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "firmware",
        "device_code_max_status_bits",
        "last_known_temp_c",
        "event_seq",
        F.sum("temp_c").over(w3).alias("device_code_sum_temp_c"),
        F.last("humidity", ignorenulls=True).over(w7).alias("last_known_humidity"),
        F.avg("status_bits").over(w8).alias("status_bits_avg_2"),
        F.sum("last_known_temp_c").over(w9).alias("last_known_temp_c_sum_30m"),
    )
)

enriched = (
    scored
    .filter(F.col("device_code") == "DV-04")
    .select(
        "id",
        "device_code",
        "temp_c",
        "firmware",
        "event_seq",
        "last_known_humidity",
        F.sum("device_code_max_status_bits").over(w7).alias("cumulative_device_code_max_status_bits"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("last_known_humidity") >= 73, "PREMIUM").otherwise("STANDARD").alias("last_known_humidity_tier"),
    )
    .agg(
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
    )
)
