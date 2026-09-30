from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("coupon_codes_count").desc_nulls_first())
w2 = Window.orderBy(F.col("coupon_codes_count_rounded").desc())
w3 = Window.orderBy(F.col("coupon_codes_count").desc_nulls_last())
w4 = Window.orderBy(F.col("coupon_codes_count_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("coupon_codes_count_dense_rank").asc_nulls_first())
w6 = Window.orderBy(F.col("coupon_codes_count_rounded_cume_dist").desc())

flattened = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("attr_key").isNotNull())
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
)

valid = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "value",
        "coupon_codes_count",
        F.round("coupon_codes_count", 1).alias("coupon_codes_count_rounded"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("coupon_codes_count_rank"),
        F.cume_dist().over(w2).alias("coupon_codes_count_rounded_cume_dist"),
        F.dense_rank().over(w3).alias("coupon_codes_count_dense_rank"),
    )
)

enriched_carts = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w4).alias("coupon_codes_count_rank_cume_dist"),
        F.cume_dist().over(w5).alias("coupon_codes_count_dense_rank_cume_dist"),
        F.rank().over(w6).alias("coupon_codes_count_rounded_cume_dist_rank"),
    )
)

result = (
    enriched_carts
    .groupBy(
        F.when(F.col("coupon_codes_count_rounded") >= 86, "LARGE").otherwise("SMALL").alias("coupon_codes_count_rounded_tier"),
    )
    .agg(
        F.min("coupon_codes_count_dense_rank_cume_dist").alias("min_coupon_codes_count_dense_rank_cume_dist"),
        F.percentile_approx("coupon_codes_count_dense_rank", [0.25, 0.5, 0.75]).alias("coupon_codes_count_dense_rank_quartiles"),
        F.max("coupon_codes_count_rounded").alias("max_coupon_codes_count_rounded"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("min_coupon_codes_count_dense_rank_cume_dist").desc())
)
