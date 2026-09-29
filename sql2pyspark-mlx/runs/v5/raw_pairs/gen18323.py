from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("product_name").like("W%"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.substring("brand", 1, 2).alias("brand_prefix"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.country").alias("country"), F.col("e.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("e.stock_qty")), 2).alias("total_stock_qty"),
        F.sum(F.col("e.price")).alias("total_price"),
    )
    .orderBy(F.col("total_price").desc_nulls_last())
)
