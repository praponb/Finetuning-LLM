from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-08-08"))) & F.col("brand").isNotNull())
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
        F.initcap(F.trim("product_name")).alias("product_name_title"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.city").alias("city"), F.col("src.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.price")).alias("min_price"),
        F.max(F.col("src.cost")).alias("max_cost"),
        F.grouping_id().alias("gid"),
    )
)
