from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_bundle").desc_nulls_last())
w_running = Window.partitionBy("tag").orderBy(F.col("is_bundle").asc_nulls_first())
w3 = Window.orderBy("is_bundle_cume_dist_2_int")
w4 = Window.orderBy(F.col("is_bundle_cume_dist").desc_nulls_last())
w5 = Window.orderBy(F.col("is_bundle_cume_dist_2").desc_nulls_last())

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("tag"))

filtered = (
    flattened
    .filter(F.col("tag").isNotNull())
    .select("*", F.substring("tag", 1, 3).alias("tag_prefix"))
)

cleaned = (
    filtered
    .filter(F.col("tag").isin(["SPRING10", "WELCOME5", "VIP"]))
    .withColumn("is_bundle", F.when(F.col("tag") == "BUNDLE", 1).otherwise(0))
    .withColumn("resolved_tag", F.coalesce("tag", F.lit("DIRECT")))
)

remaining_keys = (
    cleaned
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.col("created_at") >= "2024-09-01")
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.cume_dist().over(w).alias("is_bundle_cume_dist"),
        F.dense_rank().over(w_running).alias("is_bundle_dense_rank"),
        F.cume_dist().over(w_running).alias("is_bundle_cume_dist_2"),
    )
)

filtered_carts = (
    scored
    .filter(F.col("tag").isNotNull())
    .select(
        "id",
        "tag_prefix",
        "resolved_tag",
        "is_bundle_cume_dist",
        "is_bundle_dense_rank",
        "is_bundle_cume_dist_2",
        F.when(F.col("tag").isin(["WELCOME5", "VIP"]), "Core").otherwise("Other").alias("tag_group"),
        F.col("is_bundle_cume_dist_2").cast("int").alias("is_bundle_cume_dist_2_int"),
    )
)

scored_carts = (
    filtered_carts
    .select(
        "*",
        F.cume_dist().over(w3).alias("is_bundle_cume_dist_2_int_cume_dist"),
        F.rank().over(w4).alias("is_bundle_cume_dist_rank"),
        F.cume_dist().over(w5).alias("is_bundle_cume_dist_2_cume_dist"),
    )
)

result = (
    scored_carts
    .select(
        "id",
        "resolved_tag",
        "is_bundle_cume_dist",
        "is_bundle_dense_rank",
        "is_bundle_cume_dist_2",
        "tag_group",
        "is_bundle_cume_dist_2_int",
        "is_bundle_cume_dist_2_int_cume_dist",
        "is_bundle_cume_dist_rank",
    )
    .orderBy(F.col("is_bundle_cume_dist_rank").asc_nulls_last())
)
