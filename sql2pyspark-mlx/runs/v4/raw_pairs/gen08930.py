from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "left")
    .filter(F.col("ibp.cnt") >= 9)
    .select(F.col("p.id"), F.col("p.brand"), F.col("ibp.cnt"))
    .orderBy("brand")
)
