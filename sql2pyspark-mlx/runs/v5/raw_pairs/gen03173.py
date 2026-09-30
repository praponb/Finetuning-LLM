from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("products.category").isin(["Grocery", "Books"]))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("products.supplier_id"),
        F.col("suppliers.city"),
        F.ceil(F.col("products.price")).alias("price_whole"),
    )
)
