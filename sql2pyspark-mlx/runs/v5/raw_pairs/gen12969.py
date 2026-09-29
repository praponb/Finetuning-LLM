from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Globex"))))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        (F.col("stock_qty") + F.col("cost")).alias("stock_qty_plus_cost"),
        F.round((F.col("cost") * 0.85), 2).alias("adjusted_cost"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id") == 6), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.revenue") >= 3400), "left_semi")
)

not_flagged_products = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.units").between(13, 16)), "left_anti")
)

valid_products = (
    not_flagged_products
    .filter(F.col("cost") >= 590)
    .select(
        "*",
        F.sort_array("tags").alias("sorted_tags"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

result = (
    valid_products
    .withColumn("stock_qty_plus_cost_tier", F.when(F.col("stock_qty_plus_cost") >= 75, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["category", "stock_qty_plus_cost_tier"], ["category"]],
        "category",
        "stock_qty_plus_cost_tier",
    )
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
        F.percentile_approx("stock_qty_plus_cost", [0.25, 0.5, 0.75]).alias("stock_qty_plus_cost_quartiles"),
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90"),
    )
    .filter(F.col("initech_count") > 1)
    .orderBy(F.col("category").desc_nulls_last())
)
