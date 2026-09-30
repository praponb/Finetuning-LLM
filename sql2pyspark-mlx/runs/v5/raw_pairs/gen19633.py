from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")

metrics = (
    df_device_readings
    .select(
        "id",
        "humidity",
        "pressure",
        F.min("temp_c").over(w_recent).alias("last4_min_temp_c"),
        F.max("temp_c").over(w_recent).alias("last4_max_temp_c"),
        F.avg("temp_c").over(w_recent).alias("last4_avg_temp_c"),
        F.sum("temp_c").over(w_recent).alias("last4_sum_temp_c"),
        F.lag("pressure", 1).over(w2).alias("prev_pressure"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("prev_pressure") >= 1032, "HIGH").otherwise("LOW").alias("prev_pressure_tier"),
    )
    .agg(
        F.sum("last4_max_temp_c").alias("total_last4_max_temp_c"),
        F.percentile_approx("prev_pressure", [0.25, 0.5, 0.75]).alias("prev_pressure_quartiles"),
        F.percentile_approx("last4_sum_temp_c", [0.25, 0.5, 0.75]).alias("last4_sum_temp_c_quartiles"),
        F.percentile_approx("last4_max_temp_c", [0.25, 0.5, 0.75]).alias("last4_max_temp_c_quartiles"),
        F.avg("last4_max_temp_c").alias("avg_last4_max_temp_c"),
    )
    .filter(F.col("avg_last4_max_temp_c") < 1000000)
    .orderBy(F.col("total_last4_max_temp_c").asc_nulls_last())
)
