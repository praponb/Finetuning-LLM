from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select("*", F.ceil("temp_c").alias("temp_c_whole"))
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_gaps = (
    ranked
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w3))

ranked_device_readings = (
    sessions_device_readings
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.max("reading_ts").alias("last_reading_ts"),
        F.sum("status_bits").alias("total_status_bits"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
    )
    .orderBy("total_status_bits")
)
