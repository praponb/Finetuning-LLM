from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("quantity") > 232)
    .groupBy("product_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(F.col("p.id"), F.col("p.category"), F.col("pt.total_count"))
    .orderBy("category", F.col("total_count").desc())
)
