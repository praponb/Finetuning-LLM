from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.partitionBy("code").orderBy(F.col("code_length").desc_nulls_first())
w3 = Window.partitionBy("code").orderBy(F.col("code_max_code_length_2").desc())
w4 = Window.orderBy(F.col("code_length_2").asc_nulls_last())

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

exploded = flattened.select("id", F.explode_outer("coupon_codes").alias("code"))

valid = (
    exploded
    .filter(F.col("code").isNotNull())
    .withColumn("code_length", F.length("code"))
    .withColumn("code_length_2", F.length("code"))
)

enriched = (
    valid
    .select(
        "*",
        F.max("code_length_2").over(w).alias("code_max_code_length_2"),
        F.dense_rank().over(w2).alias("code_length_dense_rank"),
    )
)

metrics = (
    enriched
    .withColumn("code_avg_code_length", F.avg("code_length").over(w))
    .withColumn("code_stddev_samp_code_length_2", F.stddev_samp("code_length_2").over(w))
)

enriched_carts = (
    metrics
    .withColumn("code_length_dense_rank_share", F.round((F.col("code_length_dense_rank") / F.sum("code_length_dense_rank").over(w)), 4))
    .withColumn("code_count_code_avg_code_length", F.count("code_avg_code_length").over(w))
    .withColumn("code_max_code_length_2_dense_rank", F.dense_rank().over(w3))
    .withColumn("code_max_code_stddev_samp_code_length_2", F.max("code_stddev_samp_code_length_2").over(w))
    .withColumn("code_length_2_dense_rank", F.dense_rank().over(w4))
)

remaining_keys = (
    enriched_carts
    .select("id")
    .intersect(
        df_carts
        .filter(F.array_contains("coupon_codes", "BUNDLE"))
        .select("id")
    )
)

reconciled = (
    enriched_carts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    reconciled
    .groupBy(
        F.when(F.col("code_max_code_length_2") >= 90, "LARGE").otherwise("SMALL").alias("code_max_code_length_2_tier"),
    )
    .agg(
        F.count_if(F.col("code") == "SPRING10").alias("spring10_events"),
        F.sum("code_stddev_samp_code_length_2").alias("total_code_stddev_samp_code_length_2"),
        F.count_if(F.col("code") == "SPRING10").alias("spring10_events_2"),
        F.percentile_approx("code_avg_code_length", 0.95).alias("code_avg_code_length_p95"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select(
        "code_max_code_length_2_tier",
        "spring10_events",
        "total_code_stddev_samp_code_length_2",
        "spring10_events_2",
        "code_avg_code_length_p95",
    )
)
