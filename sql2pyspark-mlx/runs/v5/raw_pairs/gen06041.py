from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w_running = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("prev_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("prev_temp_c", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("status_bits_avg_5_prior", "id")
w_ordered = Window.partitionBy("device_code").orderBy("temp_c_avg_6_prior", "id")
w7 = Window.partitionBy("device_code").orderBy("temp_c_avg_6_prior")
w8 = Window.partitionBy("device_code")
w9 = Window.orderBy(F.col("temp_c").desc(), "id")
w10 = Window.orderBy("status_bits_avg_5_prior", "id").rowsBetween(-6, -1)
w11 = Window.orderBy("device_code_min_last_known_status_bits_avg_5_prior", "id")
w12 = Window.partitionBy("device_code").orderBy(F.col("device_code_min_last_known_status_bits_avg_5_prior").desc(), "id")
w13 = Window.partitionBy("device_code").orderBy("pressure", "id")

scored = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.lag("temp_c", 1).over(w).alias("prev_temp_c"),
        F.avg("status_bits").over(w_running).alias("status_bits_avg_5_prior"),
    )
)

metrics = (
    scored
    .filter(F.to_date("reading_ts") >= "2023-12-01")
    .withColumn("last_known_status_bits_avg_5_prior", F.last("status_bits_avg_5_prior", ignorenulls=True).over(w3))
    .withColumn("temp_c_avg_6_prior", F.avg("temp_c").over(w4))
)

enriched = (
    metrics
    .select(
        "*",
        F.lag("prev_temp_c", 2).over(w5).alias("prev_prev_temp_c"),
        F.ntile(5).over(w_ordered).alias("temp_c_avg_6_prior_bucket"),
        F.dense_rank().over(w7).alias("temp_c_avg_6_prior_dense_rank"),
        F.min("last_known_status_bits_avg_5_prior").over(w8).alias("device_code_min_last_known_status_bits_avg_5_prior"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

enriched_device_readings = (
    enriched
    .select(
        "*",
        F.avg("prev_prev_temp_c").over(w10).alias("prev_prev_temp_c_avg_6_prior"),
        F.lead("device_code_min_last_known_status_bits_avg_5_prior", 2).over(w11).alias("next_device_code_min_last_known_status_bits_avg_5_prior"),
        F.ntile(3).over(w12).alias("device_code_min_last_known_status_bits_avg_5_prior_bucket"),
        F.lead("temp_c_avg_6_prior_bucket", 2).over(w13).alias("next_temp_c_avg_6_prior_bucket"),
    )
)

totals = (
    enriched_device_readings
    .groupBy("seq_no")
    .agg(
        F.max("reading_ts").alias("ended_at"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.count("*").alias("event_count"),
        F.round(F.avg("next_temp_c_avg_6_prior_bucket"), 2).alias("avg_next_temp_c_avg_6_prior_bucket"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    totals
    .select(
        "seq_no",
        "ended_at",
        "num_device_codes",
        "duration_min",
        "dv_04_rows",
        "event_count",
        "avg_next_temp_c_avg_6_prior_bucket",
    )
    .orderBy(F.col("dv_04_rows").desc())
)
