from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter((F.col("suppliers.country") != "JP") & (F.col("products.price") < 490))
    .select(
        F.col("products.brand"),
        F.col("products.category"),
        F.col("products.stock_qty"),
        F.col("suppliers.city"),
    )
)
