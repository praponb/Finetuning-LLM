from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("level", "key").orderBy(F.col("flag_bit4").asc_nulls_last())
w3 = Window.partitionBy("level").orderBy(F.col("status_code").desc())
w4 = Window.orderBy("status_code")
w5 = Window.partitionBy("level").orderBy(F.col("flag_bit4").desc_nulls_first())
w6 = Window.orderBy(F.col("least_status_code_dense_rank_flag_bit4_rank").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("status_code") == 305)
    .select(
        "id",
        "level",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    unpacked
    .select(
        "id",
        "status_code",
        F.stddev_samp("status_code_high").over(w).alias("key_stddev_samp_status_code_high"),
        F.cume_dist().over(w2).alias("flag_bit4_cume_dist"),
        F.dense_rank().over(w3).alias("status_code_dense_rank"),
        F.rank().over(w4).alias("status_code_rank"),
        F.rank().over(w5).alias("flag_bit4_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("flag_bit4_rank") != 5)
    .select(
        "id",
        "status_code",
        "flag_bit4_cume_dist",
        "status_code_dense_rank",
        "status_code_rank",
        "flag_bit4_rank",
        F.floor("status_code_dense_rank").alias("status_code_dense_rank_whole"),
        (F.col("status_code") - F.col("flag_bit4_cume_dist")).alias("status_code_minus_flag_bit4_cume_dist"),
    )
)

cleaned = (
    filtered
    .filter(F.col("flag_bit4_rank") == 8)
    .select(
        "id",
        "status_code",
        "flag_bit4_cume_dist",
        "status_code_rank",
        "flag_bit4_rank",
        "status_code_dense_rank_whole",
        F.least("status_code_dense_rank", "flag_bit4_rank").alias("least_status_code_dense_rank_flag_bit4_rank"),
    )
)

enriched = (
    cleaned
    .filter(F.col("flag_bit4_cume_dist") <= 0.7)
    .withColumn("least_status_code_dense_rank_flag_bit4_rank_dense_rank", F.dense_rank().over(w6))
)

cleaned_app_logs = (
    enriched
    .filter(F.col("flag_bit4_cume_dist") > 0.51)
    .select(
        "id",
        "status_code",
        "status_code_rank",
        "flag_bit4_rank",
        "status_code_dense_rank_whole",
        "least_status_code_dense_rank_flag_bit4_rank",
        "least_status_code_dense_rank_flag_bit4_rank_dense_rank",
        (F.col("status_code_rank") + F.col("least_status_code_dense_rank_flag_bit4_rank_dense_rank")).alias("status_code_rank_plus_least_status_code_dense_rank_flag_bit4_rank_dense_rank"),
        (F.col("flag_bit4_rank") * F.col("least_status_code_dense_rank_flag_bit4_rank")).alias("flag_bit4_rank_times_least_status_code_dense_rank_flag_bit4_rank"),
    )
)

grouped = (
    cleaned_app_logs
    .groupBy(
        F.when(F.col("status_code_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("status_code_rank_tier"),
    )
    .agg(
        F.round(F.avg("least_status_code_dense_rank_flag_bit4_rank_dense_rank"), 2).alias("avg_least_status_code_dense_rank_flag_bit4_rank_dense_rank"),
        F.percentile_approx("flag_bit4_rank", 0.9).alias("flag_bit4_rank_p90"),
        F.count("*").alias("event_count"),
        F.percentile_approx("flag_bit4_rank", [0.25, 0.5, 0.75]).alias("flag_bit4_rank_quartiles"),
        F.round(F.avg("flag_bit4_rank_times_least_status_code_dense_rank_flag_bit4_rank"), 2).alias("avg_flag_bit4_rank_times_least_status_code_dense_rank_flag_bit4_rank"),
        F.bit_and("status_code").alias("status_code_bit_and"),
    )
)

result = (
    grouped
    .select(
        "status_code_rank_tier",
        "avg_least_status_code_dense_rank_flag_bit4_rank_dense_rank",
        "flag_bit4_rank_p90",
        "event_count",
        "avg_flag_bit4_rank_times_least_status_code_dense_rank_flag_bit4_rank",
    )
    .orderBy(F.col("event_count").desc_nulls_last())
)
