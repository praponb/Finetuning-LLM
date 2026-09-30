from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("units") >= 16)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit_2"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.cost"))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.stock_qty"), F.col("b.category"))
)

result = (
    with_dims
    .groupBy("category")
    .agg(
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.size(F.collect_set("category")).alias("num_categories_2"),
    )
    .orderBy(F.col("sports_count").desc())
)
