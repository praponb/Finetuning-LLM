from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.month("launch_date").isin([4, 7, 8]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        (F.col("cost") / F.nullif("stock_qty", F.lit(0))).alias("cost_per_stock_qty"),
    )
)

result = (
    filtered
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .rollup(F.col("p.country").alias("country"), F.col("e.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.cost_per_stock_qty")).alias("avg_cost_per_stock_qty"),
        F.max(F.col("e.price")).alias("max_price"),
        F.grouping_id().alias("gid"),
    )
)
