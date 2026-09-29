from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.filter(F.col("quantity") >= 179).alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_anti")
    .select(F.col("products.id"), F.col("products.cost"))
    .orderBy("cost")
)
