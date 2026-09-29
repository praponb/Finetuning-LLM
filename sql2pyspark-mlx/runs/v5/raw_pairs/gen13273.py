from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.quantity").between(163, 226))
    .groupBy(F.col("p.category"))
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
)
