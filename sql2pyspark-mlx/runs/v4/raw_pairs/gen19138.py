from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.quantity_range") > 79)
    .select(F.col("p.supplier_id"), F.col("p.launch_date"), F.col("ps.quantity_range"))
    .orderBy("quantity_range")
)
