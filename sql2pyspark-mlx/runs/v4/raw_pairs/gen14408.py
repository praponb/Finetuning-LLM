from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.unique_stores") > 8)
    .select(F.col("p.stock_qty"), F.col("pt.unique_stores"))
    .orderBy(F.col("unique_stores").desc(), "stock_qty")
)
