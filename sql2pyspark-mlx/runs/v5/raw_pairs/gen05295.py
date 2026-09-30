from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_first())
w2 = Window.orderBy(F.col("rating_dense_rank").asc_nulls_first())
w_part = Window.orderBy(F.col("rating_dense_rank_cume_dist").desc())

remaining_keys = (
    df_suppliers
    .select("city")
    .subtract(
        df_suppliers
        .filter(F.col("city") != "Berlin")
        .select("city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("supplier_name").isNotNull())
    .select("id", "supplier_name", F.dense_rank().over(w).alias("rating_dense_rank"))
)

kept = (
    scored
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isNotNull()), "left_semi")
)

scored_suppliers = (
    kept
    .select(
        "id",
        "supplier_name",
        "rating_dense_rank",
        F.cume_dist().over(w2).alias("rating_dense_rank_cume_dist"),
    )
)

scored_suppliers_2 = (
    scored_suppliers
    .withColumn("rating_dense_rank_cume_dist_percent_rank", F.percent_rank().over(w_part))
    .withColumn("rating_dense_rank_cume_dist_percent_rank_2", F.percent_rank().over(w_part))
)

filtered = (
    scored_suppliers_2
    .filter(F.col("rating_dense_rank_cume_dist_percent_rank").isNull())
    .select(
        "id",
        "supplier_name",
        "rating_dense_rank_cume_dist_percent_rank_2",
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("rating_dense_rank_cume_dist_percent_rank_2") >= 0.78, "PREMIUM").otherwise("STANDARD").alias("rating_dense_rank_cume_dist_percent_rank_2_tier"),
    )
    .agg(
        F.min("rating_dense_rank_cume_dist_percent_rank_2").alias("min_rating_dense_rank_cume_dist_percent_rank_2"),
        F.percentile_approx("rating_dense_rank_cume_dist_percent_rank_2", [0.25, 0.5, 0.75]).alias("rating_dense_rank_cume_dist_percent_rank_2_quartiles"),
        F.round(F.avg("rating_dense_rank_cume_dist_percent_rank_2"), 2).alias("avg_rating_dense_rank_cume_dist_percent_rank_2"),
        F.round(F.avg("rating_dense_rank_cume_dist_percent_rank_2"), 2).alias("avg_rating_dense_rank_cume_dist_percent_rank_2_2"),
    )
    .orderBy(F.col("rating_dense_rank_cume_dist_percent_rank_2_tier").desc_nulls_last())
)
