from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category")
w2 = Window.partitionBy("category")

cleaned = (
    df_products
    .filter(F.col("launch_date") >= "2023-04-30")
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "tags",
        "supplier_id",
        F.col("cost").cast("int").alias("cost_int"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

matched_keys = (
    cleaned
    .select("supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id") == 16)
        .select("supplier_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id") == 27), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.units").between(12, 23)), "left_anti")
)

kept_products = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock").between("2024-06-12", "2025-09-01")), "left_anti")
)

enriched = (
    kept_products
    .select(
        "*",
        F.min("cost_int").over(w).alias("supplier_id_min_cost_int"),
        F.avg("stock_qty").over(w2).alias("category_avg_stock_qty"),
    )
)

result = (
    enriched
    .groupBy(
        "category",
        F.when(F.col("supplier_id_min_cost_int") >= 66, "HIGH").otherwise("LOW").alias("supplier_id_min_cost_int_tier"),
    )
    .agg(
        F.round(F.avg("cost_int"), 2).alias("avg_cost_int"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
        F.percentile_approx("category_avg_stock_qty", 0.95).alias("category_avg_stock_qty_p95"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.avg("category_avg_stock_qty").alias("avg_category_avg_stock_qty"),
    )
)
