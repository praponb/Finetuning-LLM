from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("stock_qty") - F.col("cost")).alias("stock_qty_minus_cost"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_suppliers.alias("dim"), F.col("src.supplier_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "city"], ["country"], []],
        F.col("dim.country").alias("country"),
        F.col("dim.city").alias("city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.stock_qty_minus_cost")).alias("max_stock_qty_minus_cost"),
        F.max(F.col("src.price")).alias("max_price"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("city")
)
