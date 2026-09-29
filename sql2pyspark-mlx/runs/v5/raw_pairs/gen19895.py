from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy("status_code")
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("key", "level")
w4 = Window.partitionBy("level")
w5 = Window.orderBy(F.col("status_code_share_4").desc_nulls_last())
w6 = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w7 = Window.partitionBy("key").orderBy("status_code_share_2")
w8 = Window.partitionBy("key").orderBy(F.col("avg_key_stddev_samp_status_code_share_2").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "status_code",
        "key",
        F.cume_dist().over(w_part).alias("status_code_cume_dist"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share_2"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share_3"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share_4"),
    )
)

enriched = (
    metrics
    .withColumn("status_code_share_4_dense_rank", F.dense_rank().over(w5))
    .withColumn("key_stddev_samp_status_code_share_4", F.stddev_samp("status_code_share_4").over(w2))
)

cleaned = (
    enriched
    .filter(F.col("key") == "version")
    .withColumn("key_stddev_samp_status_code_share_4_whole", F.floor("key_stddev_samp_status_code_share_4"))
)

enriched_app_logs = (
    cleaned
    .filter(F.col("status_code_share_3") < 0.27)
    .select(
        "id",
        "key",
        "status_code_cume_dist",
        "status_code_share_2",
        "status_code_share_3",
        "key_stddev_samp_status_code_share_4",
        "key_stddev_samp_status_code_share_4_whole",
        F.round((F.col("status_code_share_4_dense_rank") / F.sum("status_code_share_4_dense_rank").over(w2)), 4).alias("status_code_share_4_dense_rank_share"),
        F.percent_rank().over(w6).alias("status_code_percent_rank"),
    )
)

filtered = (
    enriched_app_logs
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.when(F.col("status_code_percent_rank") >= 0.61, "High").when(F.col("status_code_percent_rank") >= 0.35, "Medium").otherwise("Low").alias("status_code_percent_rank_band"),
        F.length("key").alias("key_length"),
    )
)

enriched_app_logs_2 = (
    filtered
    .select(
        "id",
        "key",
        "status_code_share_3",
        F.stddev_samp("status_code_percent_rank").over(w2).alias("key_stddev_samp_status_code_percent_rank"),
        F.stddev_samp("status_code_share_2").over(w2).alias("key_stddev_samp_status_code_share_2"),
        F.count("key_stddev_samp_status_code_share_4").over(w2).alias("key_count_key_stddev_samp_status_code_share_4"),
        F.cume_dist().over(w7).alias("status_code_share_2_cume_dist"),
    )
)

summary = (
    enriched_app_logs_2
    .groupBy(
        "key",
        F.when(F.col("key_count_key_stddev_samp_status_code_share_4") >= 15, "HIGH").otherwise("LOW").alias("key_count_key_stddev_samp_status_code_share_4_tier"),
        F.when(F.col("key_stddev_samp_status_code_percent_rank") >= 1, "PREMIUM").otherwise("STANDARD").alias("key_stddev_samp_status_code_percent_rank_tier"),
    )
    .agg(
        F.avg("key_stddev_samp_status_code_share_2").alias("avg_key_stddev_samp_status_code_share_2"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
    )
    .withColumn("avg_key_stddev_samp_status_code_share_2_rank", F.rank().over(w8))
)

result = (
    summary
    .select(
        "key",
        "key_count_key_stddev_samp_status_code_share_4_tier",
        "key_stddev_samp_status_code_percent_rank_tier",
        "avg_key_stddev_samp_status_code_share_2",
        "region_count",
    )
)
