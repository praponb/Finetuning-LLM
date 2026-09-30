from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("store_id") == 3).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .filter(F.col("p.supplier_id").isNotNull())
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.price"), F.col("p.launch_date"))
)
