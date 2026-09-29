from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.stock_qty"),
        F.col("products.product_name"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
    )
    .orderBy("id")
)
