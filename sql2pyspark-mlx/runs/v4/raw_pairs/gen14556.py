from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("k")
w2 = Window.orderBy(F.col("is_weight_kg").asc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("is_weight_kg").desc_nulls_first())
w4 = Window.partitionBy("tag")

lines = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("k", "v"))

exploded = lines.select("id", "k", F.explode("coupon_codes").alias("tag"))

filtered = (
    exploded
    .filter(~F.col("k").isin(["gift", "device"]))
    .withColumn("is_weight_kg", F.when(F.col("k") == "weight_kg", 1).otherwise(0))
    .withColumn("tag_filled", F.coalesce("tag", F.lit("DIRECT")))
)

scored = (
    filtered
    .select(
        "*",
        F.min("is_weight_kg").over(w_ordered).alias("k_min_is_weight_kg"),
        F.cume_dist().over(w2).alias("is_weight_kg_cume_dist"),
        F.percent_rank().over(w3).alias("is_weight_kg_percent_rank"),
    )
)

scored_carts = (
    scored
    .select(
        "*",
        F.stddev_samp("k_min_is_weight_kg").over(w4).alias("tag_stddev_samp_k_min_is_weight_kg"),
        F.sum("is_weight_kg_percent_rank").over(w_ordered).alias("k_sum_is_weight_kg_percent_rank"),
        F.count("k_min_is_weight_kg").over(w_ordered).alias("k_count_k_min_is_weight_kg"),
    )
)

with_windows = (
    scored_carts
    .withColumn("k_max_is_weight_kg", F.max("is_weight_kg").over(w_ordered))
    .withColumn("k_stddev_samp_k_sum_is_weight_kg_percent_rank", F.stddev_samp("k_sum_is_weight_kg_percent_rank").over(w_ordered))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("k_stddev_samp_k_sum_is_weight_kg_percent_rank") >= 3, "HIGH").otherwise("LOW").alias("k_stddev_samp_k_sum_is_weight_kg_percent_rank_tier"),
        "tag",
        "k",
    )
    .agg(
        F.percentile_approx("is_weight_kg_cume_dist", 0.95).alias("is_weight_kg_cume_dist_p95"),
        F.count("*").alias("num_rows"),
        F.avg("k_max_is_weight_kg").alias("avg_k_max_is_weight_kg"),
    )
    .filter(F.col("is_weight_kg_cume_dist_p95") < 0)
    .orderBy(F.col("avg_k_max_is_weight_kg").desc_nulls_last())
)
