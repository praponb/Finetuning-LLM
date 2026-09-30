from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy(F.col("element").asc_nulls_first())
w_ordered = Window.orderBy(F.col("element_plus_element_dense_rank_2").desc())
w4 = Window.orderBy(F.col("element_plus_element_dense_rank_2").desc_nulls_first())

exploded = df_price_tiers.select("id", F.explode("discounts").alias("element"))

with_windows = (
    exploded
    .withColumn("element_dense_rank", F.dense_rank().over(w))
    .withColumn("element_cume_dist", F.cume_dist().over(w2))
    .withColumn("element_dense_rank_2", F.dense_rank().over(w))
    .withColumn("element_rank", F.rank().over(w2))
)

base = (
    with_windows
    .filter(F.col("element_dense_rank") <= 5)
    .select(
        "*",
        (F.col("element") + F.col("element_dense_rank_2")).alias("element_plus_element_dense_rank_2"),
    )
)

remaining_keys = (
    base
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") != "basic")
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(F.col("element_dense_rank_2").between(3, 7))
    .select(
        "*",
        F.rank().over(w_ordered).alias("element_plus_element_dense_rank_2_rank"),
        F.rank().over(w4).alias("element_plus_element_dense_rank_2_rank_2"),
    )
)

base_price_tiers = (
    enriched
    .filter(F.col("element_plus_element_dense_rank_2_rank").isNotNull())
    .select(
        "id",
        "element_dense_rank",
        "element_cume_dist",
        "element_dense_rank_2",
        "element_rank",
        "element_plus_element_dense_rank_2",
        "element_plus_element_dense_rank_2_rank_2",
        F.round((F.col("element_plus_element_dense_rank_2_rank") / F.col("element_rank")), 2).alias("element_plus_element_dense_rank_2_rank_per_element_rank"),
        F.round("element_plus_element_dense_rank_2_rank_2", 0).alias("element_plus_element_dense_rank_2_rank_2_rounded"),
    )
)

totals = (
    base_price_tiers
    .groupBy(
        F.when(F.col("element_cume_dist") >= 0.83, "PREMIUM").otherwise("STANDARD").alias("element_cume_dist_tier"),
    )
    .agg(
        F.min("element_plus_element_dense_rank_2_rank_per_element_rank").alias("min_element_plus_element_dense_rank_2_rank_per_element_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("element_plus_element_dense_rank_2_rank_2"), 2).alias("avg_element_plus_element_dense_rank_2_rank_2"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)

result = (
    totals
    .select(
        "element_cume_dist_tier",
        "min_element_plus_element_dense_rank_2_rank_per_element_rank",
        "distinct_ids",
        "avg_element_plus_element_dense_rank_2_rank_2",
        "distinct_ids_2",
    )
)
