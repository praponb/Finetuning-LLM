from pyspark.sql import functions as F

joined = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.rating"), F.col("s.city"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.rating").alias("supplier_rating"))
)

kept = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") >= 6), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
    )
    .orderBy(F.col("num_categories").desc())
)
