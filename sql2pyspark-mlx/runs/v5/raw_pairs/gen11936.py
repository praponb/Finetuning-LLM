from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("session_no", "id")
w_hist = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("is_new_burst", "id")
w7 = Window.partitionBy("device_code").orderBy(F.col("last_known_humidity").desc_nulls_first())
w8 = Window.partitionBy("device_code")
w9 = Window.orderBy("pressure", "id")
w_grp = Window.partitionBy("device_code").orderBy(F.col("is_new_burst").desc(), "id")
w11 = Window.partitionBy("device_code").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy("temp_c_share", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("last_known_pressure", "id")
w14 = Window.partitionBy("device_code").orderBy("last_known_humidity", "id")
w15 = Window.orderBy("prev_temp_c_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w16 = Window.orderBy("next_temp_c_share_2", "id")
w17 = Window.orderBy("next_temp_c_share_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w18 = Window.orderBy("last_known_humidity", "id")
w19 = Window.orderBy("last_known_humidity_dense_rank", "id").rowsBetween(-3, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

scored = (
    sessionized
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "firmware",
        "is_new_burst",
        F.row_number().over(w_running).alias("row_seq"),
        F.last("humidity", ignorenulls=True).over(w_hist).alias("last_known_humidity"),
        F.last("pressure", ignorenulls=True).over(w5).alias("last_known_pressure"),
    )
)

enriched = (
    scored
    .filter(F.col("last_known_pressure").between(995, 1018))
    .withColumn("is_new_burst_bucket", F.ntile(10).over(w6))
    .withColumn("last_known_humidity_dense_rank", F.dense_rank().over(w7))
    .withColumn("temp_c_share", F.round((F.col("temp_c") / F.sum("temp_c").over(w8)), 4))
    .withColumn("pressure_bucket", F.ntile(4).over(w9))
)

metrics = (
    enriched
    .withColumn("is_new_burst_bucket_2", F.ntile(5).over(w_grp))
    .withColumn("cumulative_last_known_humidity", F.sum("last_known_humidity").over(w11))
    .withColumn("last_known_humidity_dense_rank_avg_5", F.avg("last_known_humidity_dense_rank").over(w_part))
)

enriched_device_readings = (
    metrics
    .select(
        "id",
        "firmware",
        "is_new_burst",
        "last_known_humidity",
        "is_new_burst_bucket",
        "last_known_humidity_dense_rank",
        "cumulative_last_known_humidity",
        "last_known_humidity_dense_rank_avg_5",
        F.lead("is_new_burst_bucket_2").over(w_ordered).alias("next_is_new_burst_bucket_2"),
        F.lag("temp_c_share").over(w_ordered).alias("prev_temp_c_share"),
        F.lead("temp_c_share").over(w_ordered).alias("next_temp_c_share"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.lead("temp_c_share", 1).over(w14).alias("next_temp_c_share_2"),
    )
)

with_windows = (
    enriched_device_readings
    .select(
        "*",
        F.last("next_temp_c_share", ignorenulls=True).over(w15).alias("last_known_next_temp_c_share"),
        F.row_number().over(w16).alias("event_seq_2"),
        F.first("is_new_burst_bucket", ignorenulls=True).over(w17).alias("first_known_is_new_burst_bucket"),
        F.ntile(4).over(w18).alias("last_known_humidity_bucket"),
        F.avg("next_temp_c_share_2").over(w19).alias("next_temp_c_share_2_avg_3"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") < "2024-09-18")
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "is_new_burst_bucket",
        "last_known_humidity_dense_rank",
        "cumulative_last_known_humidity",
        "last_known_humidity_dense_rank_avg_5",
        "next_temp_c_share",
        "event_seq",
        "next_temp_c_share_2",
        "last_known_next_temp_c_share",
    )
    .orderBy(F.col("last_known_humidity_dense_rank_avg_5").desc())
)
