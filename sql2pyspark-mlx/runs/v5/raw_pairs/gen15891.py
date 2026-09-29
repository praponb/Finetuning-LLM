from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .filter(F.col("p.price") >= 1190)
    .select(F.col("p.id"), F.col("p.category"), F.col("p.launch_date"))
    .orderBy("category")
)
