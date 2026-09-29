from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "right")
    .filter((F.col("store_sales.revenue") > 3500) | F.col("suppliers.supplier_name").isNotNull())
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.sale_date"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
    )
)
