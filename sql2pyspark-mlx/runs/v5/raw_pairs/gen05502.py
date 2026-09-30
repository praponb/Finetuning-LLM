from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("temp_c", "id")
w6 = Window.orderBy("reading_ts", "id")
w_recent = Window.orderBy("running_temp_c_filled", "id")
w8 = Window.orderBy("running_temp_c_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("prev_temp_c").desc_nulls_first())
w10 = Window.orderBy("row_num", "id")
w11 = Window.orderBy("prev_temp_c_cume_dist", "id").rowsBetween(-3, -1)

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "reading_ts", "temp_c", "firmware", "row_num")
)

metrics = (
    ranked
    .withColumn("temp_c_avg_30m", F.avg("temp_c").over(w3))
    .withColumn("running_temp_c", F.sum("temp_c").over(w4))
    .withColumn("temp_c_bucket", F.ntile(5).over(w5))
    .withColumn("prev_temp_c", F.lag("temp_c", 2).over(w6))
)

cleaned = (
    metrics
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.coalesce("running_temp_c", F.lit(0)).alias("running_temp_c_filled"),
        F.ceil("temp_c").alias("temp_c_whole"),
    )
)

metrics_device_readings = (
    cleaned
    .withColumn("next_running_temp_c_filled", F.lead("running_temp_c_filled").over(w_recent))
    .withColumn("prev_prev_temp_c", F.lag("prev_temp_c").over(w_recent))
    .withColumn("next_prev_temp_c", F.lead("prev_temp_c").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

enriched = (
    metrics_device_readings
    .filter(F.col("next_running_temp_c_filled").between(25, 29))
    .select(
        "*",
        F.sum("running_temp_c_filled").over(w8).alias("cumulative_running_temp_c_filled"),
        F.cume_dist().over(w9).alias("prev_temp_c_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "*",
        F.lead("prev_temp_c_cume_dist").over(w10).alias("next_prev_temp_c_cume_dist"),
        F.lag("prev_temp_c_cume_dist").over(w10).alias("prev_prev_temp_c_cume_dist"),
        F.lag("row_seq").over(w10).alias("prev_row_seq"),
        F.row_number().over(w10).alias("seq_no"),
        F.avg("next_running_temp_c_filled").over(w11).alias("next_running_temp_c_filled_avg_3_prior"),
    )
)
