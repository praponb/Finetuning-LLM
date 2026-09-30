from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")

matched_keys = (
    df_products
    .select("category", "supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id") == 29)
        .select("category", "supplier_id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.launch_date")), "left_anti")
)

remaining_keys = (
    kept
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("stock_qty") > 329)
        .select("id", "supplier_id")
    )
)

reconciled_products = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled_products
    .filter(F.col("cost") > 500)
    .select(
        "category",
        "brand",
        "cost",
        "stock_qty",
        "supplier_id",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.count("price").over(w).alias("brand_count_price"),
        F.min("cost").over(w).alias("brand_min_cost"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select(
        "s.brand",
        "s.cost",
        "s.stock_qty",
        "s.supplier_id",
        "s.cost_share",
        "s.brand_min_cost",
        F.col("d.city"),
        F.col("d.rating"),
        F.col("d.supplier_name"),
    )
)

result = (
    joined
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country").alias("country"), F.col("a.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.cost_share")).alias("avg_cost_share"),
        F.avg(F.col("a.stock_qty")).alias("avg_stock_qty"),
    )
)
