from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("status_bits_dense_rank", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w5 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy("status_bits_dense_rank_avg_6", "id")
w_recent = Window.partitionBy("device_code").orderBy("corr_status_bits_pressure", "id")

metrics = (
    df_device_readings
    .select(
        "*",
        F.dense_rank().over(w).alias("status_bits_dense_rank"),
        F.corr("status_bits", "pressure").over(w2).alias("corr_status_bits_pressure"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.avg("status_bits_dense_rank").over(w3).alias("status_bits_dense_rank_avg_6"),
        F.ntile(10).over(w4).alias("pressure_bucket"),
        F.sum("status_bits_dense_rank").over(w5).alias("cumulative_status_bits_dense_rank"),
    )
)

scored_device_readings = (
    scored
    .select(
        "*",
        F.lead("humidity", 2).over(w_hist).alias("next_humidity"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4).alias("humidity_share"),
    )
)

result = (
    scored_device_readings
    .groupBy("device_code")
    .agg(
        F.percentile_approx("status_bits_dense_rank_avg_6", 0.9).alias("status_bits_dense_rank_avg_6_p90"),
        F.round(F.avg("pressure_bucket"), 2).alias("avg_pressure_bucket"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("cumulative_status_bits_dense_rank")).otherwise(0)).alias("dv_04_cumulative_status_bits_dense_rank"),
        F.max("reading_ts").alias("ended_at"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
    )
    .filter(F.col("status_bits_dense_rank_avg_6_p90") > 0)
    .orderBy("status_bits_dense_rank_avg_6_p90")
)
