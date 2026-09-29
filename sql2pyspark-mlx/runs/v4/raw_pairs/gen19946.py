from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.order_id").isin([2, 14, 21, 32]))
    .select(F.col("oi.order_id"), F.col("p.launch_date"))
    .orderBy("launch_date")
)
