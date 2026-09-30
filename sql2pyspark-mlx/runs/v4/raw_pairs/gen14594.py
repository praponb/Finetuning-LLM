from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("tag")
w2 = Window.orderBy(F.col("is_bundle").desc_nulls_first())
w3 = Window.partitionBy("tag").orderBy(F.col("is_bundle_cume_dist").asc_nulls_first())
w4 = Window.partitionBy("tag").orderBy(F.col("is_bundle").desc_nulls_last())

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("tag"))

matched_keys = (
    flattened
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isin([11, 15, 35]))
        .select("id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("tag").isNotNull() & ~F.col("tag").isin(["VIP", "BUNDLE"]))
    .withColumn("clean_tag", F.trim("tag"))
)

cleaned = (
    base
    .filter(F.col("tag") != "BUNDLE")
    .select("*", F.when(F.col("tag") == "BUNDLE", 1).otherwise(0).alias("is_bundle"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.count("is_bundle").over(w_recent).alias("tag_count_is_bundle"),
        F.cume_dist().over(w2).alias("is_bundle_cume_dist"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w2).alias("is_bundle_dense_rank"),
        F.rank().over(w3).alias("is_bundle_cume_dist_rank"),
        F.percent_rank().over(w4).alias("is_bundle_percent_rank"),
    )
)

valid = (
    scored
    .filter(F.col("tag") == "SPRING10")
    .select(
        "id",
        "tag",
        "clean_tag",
        "is_bundle",
        "is_bundle_cume_dist",
        "is_bundle_cume_dist_rank",
        "is_bundle_percent_rank",
        F.round((F.col("is_bundle_cume_dist") * 0.9), 2).alias("adjusted_is_bundle_cume_dist"),
        F.coalesce("tag", F.lit("N/A")).alias("resolved_tag"),
    )
)

summary = (
    valid
    .groupBy("tag")
    .agg(
        F.avg("is_bundle_cume_dist_rank").alias("avg_is_bundle_cume_dist_rank"),
        F.round(F.avg("is_bundle_cume_dist"), 2).alias("avg_is_bundle_cume_dist"),
    )
)

result = summary.select("tag", "avg_is_bundle_cume_dist_rank", "avg_is_bundle_cume_dist")
