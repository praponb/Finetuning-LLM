from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("product_id").isin([6, 8, 10]))
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.product_name"), F.col("p.brand"), F.col("ps.cnt"))
)
