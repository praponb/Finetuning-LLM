from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "level")
w2 = Window.partitionBy("level", "key")
w_grp = Window.partitionBy("key")
w4 = Window.partitionBy("level")
w5 = Window.orderBy(F.col("status_code_share").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

parsed = (
    lines
    .select(
        "level",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.max("status_code").over(w2).alias("level_max_status_code"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_2"),
    )
)

valid = (
    metrics
    .filter(F.col("key") == "region")
    .withColumn("status_code_share_minus_status_code_high", (F.col("status_code_share") - F.col("status_code_high")))
)

metrics_app_logs = (
    valid
    .filter(F.col("status_code") >= 357)
    .withColumn("level_avg_status_code_high", F.avg("status_code_high").over(w2))
    .withColumn("key_count_status_code_high", F.count("status_code_high").over(w_grp))
    .withColumn("level_min_status_code_share_2", F.min("status_code_share_2").over(w4))
    .withColumn("key_sum_status_code_share_2", F.sum("status_code_share_2").over(w_grp))
    .withColumn("status_code_share_percent_rank", F.percent_rank().over(w5))
)

result = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("status_code_share") >= 0.77, "PREMIUM").otherwise("STANDARD").alias("status_code_share_tier"),
    )
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.round(F.avg("status_code_share_percent_rank"), 2).alias("avg_status_code_share_percent_rank"),
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.countDistinct("key").alias("distinct_keys_2"),
    )
    .orderBy(F.col("distinct_keys_2").desc_nulls_last())
)
