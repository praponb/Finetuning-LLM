from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("cost") < 340).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .select(F.col("i.id"), F.col("i.product_id"))
    .orderBy("product_id", "id")
    .limit(10)
)
