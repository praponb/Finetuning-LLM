from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("product_id").isin([1, 8, 9, 10])).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.launch_date"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
