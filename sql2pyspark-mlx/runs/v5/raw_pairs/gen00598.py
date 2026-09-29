from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("price").between(400, 1010) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.add_months("launch_date", 12).alias("launch_date_plus_months"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country").alias("country"), F.col("a.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.cost")).alias("max_cost"),
        F.min(F.col("a.stock_qty")).alias("min_stock_qty"),
    )
)
