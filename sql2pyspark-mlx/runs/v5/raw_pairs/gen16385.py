from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_gift").desc())
w2 = Window.orderBy(F.col("is_gift_adjusted").desc_nulls_last())
w3 = Window.partitionBy("k")
w_recent = Window.partitionBy("k").orderBy(F.col("is_gift_adjusted_percent_rank").asc_nulls_first())
w5 = Window.orderBy(F.col("k_count_is_gift").asc_nulls_last())
w_hist = Window.partitionBy("k").orderBy(F.col("k_count_is_gift").asc_nulls_first())

lines = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("k", "v"))

cleaned = (
    lines
    .filter(F.col("k").isNotNull())
    .withColumn("sorted_coupon_codes", F.sort_array("coupon_codes"))
    .withColumn("is_gift", F.when(F.col("k") == "gift", 1).otherwise(0))
)

base = (
    cleaned
    .filter(F.col("is_gift").between(40, 41))
    .select(
        "id",
        "k",
        "v",
        "is_gift",
        F.round((F.col("is_gift") * 1.15), 2).alias("is_gift_adjusted"),
        F.coalesce("k", F.lit("OTHER")).alias("k_clean"),
    )
)

remaining_keys = (
    base
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isNull())
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned_carts = (
    kept_rows
    .filter(F.col("k").isNotNull())
    .select("*", F.initcap(F.trim("k")).alias("k_title"))
)

enriched = (
    cleaned_carts
    .filter(F.col("is_gift_adjusted") < 38)
    .select(
        "k",
        "k_clean",
        F.cume_dist().over(w).alias("is_gift_cume_dist"),
        F.percent_rank().over(w2).alias("is_gift_adjusted_percent_rank"),
        F.count("is_gift").over(w3).alias("k_count_is_gift"),
        F.avg("is_gift").over(w3).alias("k_avg_is_gift"),
    )
)

metrics = (
    enriched
    .withColumn("is_gift_adjusted_percent_rank_rank", F.rank().over(w_recent))
    .withColumn("k_count_is_gift_percent_rank", F.percent_rank().over(w5))
    .withColumn("k_count_is_gift_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("k_stddev_samp_is_gift_cume_dist", F.stddev_samp("is_gift_cume_dist").over(w3))
)

result = (
    metrics
    .select(
        "k_clean",
        "is_gift_cume_dist",
        "is_gift_adjusted_percent_rank",
        "k_count_is_gift",
        "k_avg_is_gift",
        "is_gift_adjusted_percent_rank_rank",
        "k_count_is_gift_dense_rank",
        "k_stddev_samp_is_gift_cume_dist",
    )
)
