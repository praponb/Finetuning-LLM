from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "city")
w_part = Window.partitionBy("city").orderBy(F.col("running_avg_rating").desc())
w_hist = Window.partitionBy("city")
w5 = Window.orderBy(F.col("running_min_rating").asc_nulls_first())
w6 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())
w_recent = Window.partitionBy("country")
w8 = Window.partitionBy("rating_share_tier").orderBy(F.col("tokyo_avg_running_avg_rating_cume_dist").desc())

base = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

scored = (
    base
    .withColumn("running_count_rating", F.count("rating").over(w))
    .withColumn("running_min_rating", F.min("rating").over(w))
    .withColumn("running_avg_rating", F.avg("rating").over(w))
)

kept_keys = (
    scored
    .select("city", "id")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "Bangkok")
        .select("city", "id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored_suppliers = (
    reconciled
    .filter(F.col("country") == "TH")
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
    .withColumn("running_avg_rating_cume_dist", F.cume_dist().over(w_part))
)

metrics = (
    scored_suppliers
    .select(
        "*",
        F.round((F.col("running_count_rating") / F.sum("running_count_rating").over(w2)), 4).alias("running_count_rating_share"),
        F.stddev_samp("rating").over(w_hist).alias("city_stddev_samp_rating"),
        F.dense_rank().over(w5).alias("running_min_rating_dense_rank"),
        F.rank().over(w6).alias("rating_rank"),
        F.max("running_avg_rating_cume_dist").over(w_recent).alias("country_max_running_avg_rating_cume_dist"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id") == 28), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("rating_share") >= 0.61, "LARGE").otherwise("SMALL").alias("rating_share_tier"),
        F.when(F.col("rating") >= 3.8, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.min("running_min_rating_dense_rank").alias("min_running_min_rating_dense_rank"),
        F.avg(F.when(F.col("city") == "Tokyo", F.col("running_avg_rating_cume_dist"))).alias("tokyo_avg_running_avg_rating_cume_dist"),
        F.avg("rating_rank").alias("avg_rating_rank"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.percentile_approx("running_avg_rating_cume_dist", 0.5).alias("running_avg_rating_cume_dist_p50"),
    )
    .withColumn("tokyo_avg_running_avg_rating_cume_dist_dense_rank", F.dense_rank().over(w8))
    .orderBy(F.col("rating_tier").asc_nulls_last())
)
