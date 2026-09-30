from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("status_code_high").asc_nulls_last())
w2 = Window.orderBy(F.col("status_code_high").desc())
w3 = Window.partitionBy("k").orderBy(F.col("is_flagged").asc_nulls_first())
w4 = Window.partitionBy("k", "host").orderBy(F.col("is_flagged").desc_nulls_first())
w5 = Window.partitionBy("host")
w6 = Window.partitionBy("k", "host")
w7 = Window.orderBy(F.col("status_code_high_cume_dist_2").asc_nulls_last())
w8 = Window.orderBy(F.col("is_flagged_cume_dist").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "host",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

matched_keys = (
    flattened_attrs
    .select("id", "host")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms").isNotNull())
        .select("id", "host")
    )
)

verified = (
    flattened_attrs
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.cume_dist().over(w).alias("status_code_high_cume_dist"),
        F.cume_dist().over(w2).alias("status_code_high_cume_dist_2"),
        F.cume_dist().over(w3).alias("is_flagged_cume_dist"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.percent_rank().over(w4).alias("is_flagged_percent_rank"),
        F.sum("status_code_high_cume_dist_2").over(w5).alias("host_sum_status_code_high_cume_dist_2"),
    )
)

cleaned = (
    enriched
    .filter(F.col("k").isNotNull())
    .withColumn("is_flagged_tier", F.when(F.col("is_flagged") >= 1, "High").when(F.col("is_flagged") >= 0, "Medium").otherwise("Low"))
    .withColumn("k_filled", F.coalesce("k", F.lit("NONE")))
)

scored_app_logs = (
    cleaned
    .withColumn("k_count_is_flagged_percent_rank", F.count("is_flagged_percent_rank").over(w6))
    .withColumn("status_code_high_cume_dist_2_rank", F.rank().over(w7))
    .withColumn("is_flagged_cume_dist_cume_dist", F.cume_dist().over(w8))
    .withColumn("host_sum_is_flagged_percent_rank", F.sum("is_flagged_percent_rank").over(w5))
    .withColumn("host_min_is_flagged_cume_dist", F.min("is_flagged_cume_dist").over(w5))
)

stats = (
    scored_app_logs
    .groupBy(
        F.when(F.col("k_count_is_flagged_percent_rank") >= 16, "PREMIUM").otherwise("STANDARD").alias("k_count_is_flagged_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("host_min_is_flagged_cume_dist", [0.25, 0.5, 0.75]).alias("host_min_is_flagged_cume_dist_quartiles"),
        F.sum(F.when(F.col("host") == "h2", F.col("status_code_high"))).alias("h2_total_status_code_high"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    stats
    .select(
        "k_count_is_flagged_percent_rank_tier",
        "event_count",
        "host_min_is_flagged_cume_dist_quartiles",
    )
    .orderBy(F.col("event_count").desc(), "k_count_is_flagged_percent_rank_tier")
)
