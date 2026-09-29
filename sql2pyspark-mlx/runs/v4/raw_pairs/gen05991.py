from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.filter(F.col("store_id") == 6).alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_anti")
    .filter(F.col("products.brand") == "Stark")
    .select(F.col("products.id"), F.col("products.supplier_id"))
    .orderBy(F.col("id").desc(), "supplier_id")
)
