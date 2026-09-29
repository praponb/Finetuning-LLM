from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.filter(F.col("quantity") < 63).alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_anti")
    .select(
        F.col("products.id"),
        F.col("products.stock_qty"),
        F.col("products.supplier_id"),
        F.col("products.product_name"),
    )
    .orderBy(F.col("supplier_id").desc(), "id")
    .limit(5)
)
