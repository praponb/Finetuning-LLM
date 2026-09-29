from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy(F.col("tag_length").desc())
w2 = Window.orderBy(F.col("tag_length").asc_nulls_first())
w3 = Window.partitionBy("tag")
w4 = Window.partitionBy("tag").orderBy(F.col("tag_length_cume_dist").desc_nulls_first())
w5 = Window.partitionBy("tag").orderBy("tag_min_tag_length")
w6 = Window.orderBy(F.col("tag_length_dense_rank").desc_nulls_last())
w7 = Window.partitionBy("tag").orderBy("tag_length")

lines = df_carts.select("id", F.explode_outer("coupon_codes").alias("tag"))

valid = (
    lines
    .filter(F.col("tag").isin(["VIP", "WELCOME5", "SPRING10"]))
    .withColumn("tag_lower", F.lower("tag"))
    .withColumn("tag_length", F.length("tag"))
    .withColumn("resolved_tag", F.coalesce("tag", F.lit("N/A")))
)

metrics = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("tag_length_dense_rank"),
        F.cume_dist().over(w2).alias("tag_length_cume_dist"),
        F.min("tag_length").over(w3).alias("tag_min_tag_length"),
    )
)

metrics_carts = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w4).alias("tag_length_cume_dist_dense_rank"),
        F.dense_rank().over(w5).alias("tag_min_tag_length_dense_rank"),
        F.dense_rank().over(w6).alias("tag_length_dense_rank_dense_rank"),
    )
)

filtered = (
    metrics_carts
    .filter(F.col("tag_length_dense_rank").between(6, 8))
    .withColumn("greatest_tag_length_dense_rank_dense_rank_tag_length_dense_rank", F.greatest("tag_length_dense_rank_dense_rank", "tag_length_dense_rank"))
)

scored = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w7).alias("tag_length_dense_rank_2"),
        F.rank().over(w6).alias("tag_length_dense_rank_rank"),
        F.cume_dist().over(w6).alias("tag_length_dense_rank_cume_dist"),
        F.max("tag_length").over(w3).alias("tag_max_tag_length"),
    )
)

base = (
    scored
    .filter(F.col("tag").isNotNull())
    .select(
        "*",
        F.greatest("tag_length_cume_dist", "greatest_tag_length_dense_rank_dense_rank_tag_length_dense_rank").alias("greatest_tag_length_cume_dist_greatest_tag_length_dense_rank_dense_rank_tag_length_dense_rank"),
        F.coalesce("tag", F.lit("UNKNOWN")).alias("tag_filled"),
    )
)

result = (
    base
    .groupBy("tag")
    .agg(
        F.sum(F.when(F.col("tag") == "BUNDLE", F.col("tag_length_dense_rank_2")).otherwise(0)).alias("bundle_tag_length_dense_rank_2"),
        F.round(F.avg("tag_length_dense_rank_rank"), 2).alias("avg_tag_length_dense_rank_rank"),
        F.sum(F.when(F.col("tag") == "VIP", F.col("tag_length_cume_dist")).otherwise(0)).alias("vip_tag_length_cume_dist"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.percentile_approx("tag_length_dense_rank_cume_dist", 0.9).alias("tag_length_dense_rank_cume_dist_p90"),
    )
    .filter(F.col("bundle_tag_length_dense_rank_2") >= 0)
)
