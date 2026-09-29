from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.store_id"),
        F.col("products.cost"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
        F.when(F.col("suppliers.rating") >= 4.2, "High").when(F.col("suppliers.rating") >= 2.0, "Medium").otherwise("Low").alias("rating_band"),
    )
)
