from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("attr_key_length").desc_nulls_last())
w2 = Window.partitionBy("tag")
w3 = Window.partitionBy("attr_key").orderBy("tag_count_attr_key_length")
w4 = Window.partitionBy("attr_key").orderBy("attr_key_length_cume_dist")
w5 = Window.partitionBy("attr_key")
w6 = Window.partitionBy("tag").orderBy(F.col("attr_key_length").asc_nulls_first())

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("attr_key", "v"))
)

exploded = lines.select("id", "attr_key", F.explode("coupon_codes").alias("tag"))

cleaned = (
    exploded
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.upper("attr_key").alias("attr_key_upper"),
        F.coalesce("tag", F.lit("OTHER")).alias("tag_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.length("attr_key").alias("attr_key_length"),
        F.substring("tag", 1, 3).alias("tag_prefix"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("attr_key_length_cume_dist"),
        F.count("attr_key_length").over(w2).alias("tag_count_attr_key_length"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w3).alias("tag_count_attr_key_length_cume_dist"),
        F.percent_rank().over(w4).alias("attr_key_length_cume_dist_percent_rank"),
        F.min("attr_key_length").over(w5).alias("attr_key_min_attr_key_length"),
        F.dense_rank().over(w6).alias("attr_key_length_dense_rank"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("attr_key_length_cume_dist") >= 0.84, "HIGH").otherwise("LOW").alias("attr_key_length_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("attr_key_min_attr_key_length"), 2).alias("avg_attr_key_min_attr_key_length"),
        F.countDistinct("tag").alias("distinct_tags"),
        F.round(F.sum("tag_count_attr_key_length") / F.nullif(F.count(F.when(F.col("tag") == "BUNDLE", 1)), F.lit(0)), 2).alias("tag_count_attr_key_length_per_bundle"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
    )
)

result = (
    grouped
    .select(
        "attr_key_length_cume_dist_tier",
        "avg_attr_key_min_attr_key_length",
        "distinct_tags",
        "tag_count_attr_key_length_per_bundle",
        "distinct_attr_keys",
    )
    .orderBy("tag_count_attr_key_length_per_bundle", "attr_key_length_cume_dist_tier")
    .limit(10)
)
