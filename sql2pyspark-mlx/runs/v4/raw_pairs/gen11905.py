from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .select(F.col("i.product_id"), F.col("p.product_name"), F.col("p.brand"))
    .orderBy("brand", "product_name")
)
