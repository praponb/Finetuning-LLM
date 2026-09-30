from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.month("launch_date").isin([1, 4, 12]))
    .select(
        "id",
        "product_name",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.round((F.col("stock_qty") / F.col("cost")), 2).alias("stock_qty_per_cost"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .cube(F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.stock_qty")).alias("total_stock_qty"),
        F.round(F.avg(F.col("s.stock_qty_per_cost")), 2).alias("avg_stock_qty_per_cost"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
