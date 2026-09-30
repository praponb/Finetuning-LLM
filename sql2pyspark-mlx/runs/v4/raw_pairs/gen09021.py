from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w4 = Window.orderBy("humidity")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w7 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")

with_windows = (
    df_device_readings
    .select(
        "*",
        F.avg("humidity").over(w).alias("humidity_avg_2_prior"),
        F.sum("temp_c").over(w2).alias("running_temp_c"),
        F.ntile(5).over(w3).alias("humidity_bucket"),
        F.dense_rank().over(w4).alias("humidity_dense_rank"),
        F.first("pressure", ignorenulls=True).over(w5).alias("first_known_pressure"),
    )
)

prev_events = with_windows.withColumn("prev_reading_ts", F.lag("reading_ts").over(w6))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w5))

latest = (
    islands
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    latest
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.col("pressure").between(1006, 1018))
        .select("device_code")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "device_code",
        F.when(F.col("humidity_bucket") >= 7, "HIGH").otherwise("LOW").alias("humidity_bucket_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("is_new_burst_per_dv_04"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
