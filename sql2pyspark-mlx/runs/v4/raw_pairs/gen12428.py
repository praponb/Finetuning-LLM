from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("quantity") != 113).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.brand"), F.col("p.price"))
    .orderBy("price", F.col("brand").desc())
)
