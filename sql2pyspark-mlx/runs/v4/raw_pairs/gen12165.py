from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())
w2 = Window.orderBy("rating")
w3 = Window.partitionBy("city", "country").orderBy(F.col("rating").asc_nulls_first())
w4 = Window.orderBy(F.col("rating").desc_nulls_first())
w5 = Window.partitionBy("country")
w_running = Window.partitionBy("country", "city").orderBy(F.col("rating_cume_dist").desc_nulls_first())
w7 = Window.partitionBy("country", "city")

matched_keys = (
    df_suppliers
    .select("id", "country")
    .intersect(
        df_suppliers
        .filter(F.col("country").isin(["TH", "JP", "US"]))
        .select("id", "country")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("rating_percent_rank", F.percent_rank().over(w))
    .withColumn("rating_cume_dist", F.cume_dist().over(w2))
    .withColumn("rating_rank", F.rank().over(w3))
)

kept = (
    metrics
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.product_name").like("W%")), "left_semi")
)

metrics_suppliers = (
    kept
    .withColumn("rating_rank_2", F.rank().over(w4))
    .withColumn("country_count_rating_rank", F.count("rating_rank").over(w5))
    .withColumn("rating_cume_dist_dense_rank", F.dense_rank().over(w_running))
    .withColumn("rating_rank_share", F.round((F.col("rating_rank") / F.sum("rating_rank").over(w7)), 4))
)

valid = (
    metrics_suppliers
    .filter(F.col("rating_rank_share").isNull())
    .select(
        "supplier_name",
        "country",
        "rating",
        "rating_percent_rank",
        "rating_rank_2",
        "rating_cume_dist_dense_rank",
        "rating_rank_share",
        (F.col("rating_cume_dist") - F.col("rating_cume_dist_dense_rank")).alias("rating_cume_dist_minus_rating_cume_dist_dense_rank"),
    )
)

cleaned = (
    valid
    .filter(F.col("rating_rank_2") > 7)
    .withColumn("greatest_rating_rank_share_rating_cume_dist_dense_rank", F.greatest("rating_rank_share", "rating_cume_dist_dense_rank"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.stddev_samp("greatest_rating_rank_share_rating_cume_dist_dense_rank").over(w5).alias("country_stddev_samp_greatest_rating_rank_share_rating_cume_dist_dense_rank"),
        F.count("rating_cume_dist_minus_rating_cume_dist_dense_rank").over(w5).alias("country_count_rating_cume_dist_minus_rating_cume_dist_dense_rank"),
        F.avg("rating_rank_2").over(w5).alias("country_avg_rating_rank_2"),
    )
)

result = (
    enriched
    .groupBy("country")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
    )
    .orderBy(F.col("country").desc())
)
