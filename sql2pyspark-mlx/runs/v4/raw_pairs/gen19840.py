from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("temp_c").desc(), "id")
w5 = Window.orderBy(F.col("temp_c").desc_nulls_last())
w_running_2 = Window.orderBy("humidity", "id")
w7 = Window.partitionBy("device_code").orderBy("least_pressure_temp_c_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy(F.col("seq_no").desc())
w9 = Window.partitionBy("device_code").orderBy(F.col("first_known_temp_c").desc_nulls_first(), "id")
w_recent = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy("seq_no_dense_rank", "id")
w12 = Window.partitionBy("device_code").orderBy("corr_humidity_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("device_code").orderBy(F.col("prev_is_2_0_1").desc(), "id")

scored = (
    df_device_readings
    .withColumn("temp_c_share", F.round((F.col("temp_c") / F.sum("temp_c").over(w)), 4))
    .withColumn("next_humidity", F.lead("humidity", 2).over(w2))
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("temp_c_percent_rank", F.percent_rank().over(w5))
)

cleaned = (
    scored
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("last_known_humidity").between(63, 65))
    .select(
        "*",
        F.least("pressure", "temp_c_share").alias("least_pressure_temp_c_share"),
        F.when(F.col("firmware") == "2.0.1", 1).otherwise(0).alias("is_2_0_1"),
    )
)

with_windows = (
    cleaned
    .withColumn("corr_pressure_temp_c", F.corr("pressure", "temp_c").over(w))
    .withColumn("humidity_bucket", F.ntile(4).over(w_running_2))
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w7))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w8))
    .withColumn("corr_humidity_temp_c", F.corr("humidity", "temp_c").over(w))
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .withColumn("device_code_sum_corr_pressure_temp_c", F.sum("corr_pressure_temp_c").over(w))
    .withColumn("seq_no_dense_rank_count_2h", F.count("seq_no_dense_rank").over(w_recent))
    .withColumn("prev_is_2_0_1", F.lag("is_2_0_1", 1).over(w11))
)

enriched = (
    metrics
    .withColumn("running_temp_c_percent_rank", F.sum("temp_c_percent_rank").over(w12))
    .withColumn("last_known_least_pressure_temp_c_share", F.last("least_pressure_temp_c_share", ignorenulls=True).over(w13))
    .withColumn("prev_is_2_0_1_bucket", F.ntile(10).over(w14))
)

kept_keys = (
    enriched
    .select("device_code", "id")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") == "1.0.3")
        .select("device_code", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .groupBy("device_code")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("prev_is_2_0_1_bucket") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("prev_is_2_0_1_bucket_per_dv_01"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
    )
)

result = summary.select("device_code", "cnt", "prev_is_2_0_1_bucket_per_dv_01")
