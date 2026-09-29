from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w5 = Window.partitionBy("device_code").orderBy("is_new_session", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("device_code")
w8 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last())
w10 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy(F.col("corr_pressure_status_bits").asc_nulls_first())

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

gaps = (
    deduped
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_session").over(w3))

enriched = (
    islands
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.avg("is_new_session").over(w5).alias("is_new_session_avg_6"),
        F.avg("humidity").over(w6).alias("humidity_avg_5_prior"),
        F.corr("pressure", "status_bits").over(w7).alias("corr_pressure_status_bits"),
        F.max("is_new_session").over(w8).alias("is_new_session_max_1h"),
    )
)

kept_keys = (
    enriched
    .select("firmware", "id")
    .subtract(
        df_device_readings
        .filter(F.hour("reading_ts").between(7, 13))
        .select("firmware", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_device_readings = (
    verified
    .select(
        "*",
        F.round((F.col("humidity") / F.sum("humidity").over(w7)), 4).alias("humidity_share"),
        F.rank().over(w9).alias("temp_c_rank"),
        F.count("row_seq").over(w10).alias("row_seq_count_2h"),
        F.rank().over(w11).alias("corr_pressure_status_bits_rank"),
        F.count("is_new_session_max_1h").over(w7).alias("device_code_count_is_new_session_max_1h"),
    )
)

cleaned = (
    enriched_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("humidity_avg_5_prior") != 54))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "is_new_session",
        "session_id",
        "row_seq",
        "is_new_session_avg_6",
        "humidity_avg_5_prior",
        "corr_pressure_status_bits",
        "is_new_session_max_1h",
        "temp_c_rank",
        "row_seq_count_2h",
        "device_code_count_is_new_session_max_1h",
        F.lower("firmware").alias("firmware_lower"),
    )
)

stats = (
    cleaned
    .groupBy(
        F.when(F.col("is_new_session_max_1h") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_max_1h_tier"),
    )
    .agg(
        F.avg("is_new_session_avg_6").alias("avg_is_new_session_avg_6"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.max(F.when(F.col("device_code") == "DV-03", F.col("status_bits"))).alias("dv_03_max_status_bits"),
    )
)

result = (
    stats
    .select(
        "is_new_session_max_1h_tier",
        "avg_is_new_session_avg_6",
        "dv_02_events",
        "dv_03_max_status_bits",
    )
    .orderBy(F.col("avg_is_new_session_avg_6").desc())
)
