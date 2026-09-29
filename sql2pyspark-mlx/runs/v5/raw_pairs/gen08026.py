from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("launch_date") <= "2023-03-01") & (F.col("launch_date") > F.to_date(F.lit("2023-03-18"))))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("price") - F.col("stock_qty")).alias("price_minus_stock_qty"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .filter(F.col("p.country") == "TH")
    .select(
        "a.id",
        "a.category",
        "a.brand",
        "a.price",
        "a.stock_qty",
        "a.launch_date",
        "a.tags",
        "a.supplier_id",
        F.col("p.supplier_name"),
        F.col("p.rating"),
    )
)
