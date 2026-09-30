from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("session_no").desc(), "id")
w4 = Window.partitionBy("hour_window").orderBy(F.col("session_no_per_dv_02").desc())

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select("*", F.ceil("status_bits").alias("status_bits_whole"))
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

ranked = (
    sessions_device_readings
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    ranked
    .filter(F.col("status_bits_whole").isNull())
    .select("*", F.round("humidity", 0).alias("humidity_rounded"))
)

with_gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("temp_c") >= 38.4, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
    )
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("session_no_per_dv_02"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.sum("session_no").alias("total_session_no"),
    )
    .filter(F.col("total_session_no") > 0)
    .withColumn("session_no_per_dv_02_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("total_session_no").desc())
)
