from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("category", "id")
    .subtract(
        df_products
        .filter(F.col("stock_qty").between(286, 320))
        .select("category", "id")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("category")
    .agg(
        F.avg("price").alias("avg_price"),
        F.percentile_approx("price", 0.5).alias("price_p50"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .orderBy(F.col("avg_price").desc_nulls_first())
)
