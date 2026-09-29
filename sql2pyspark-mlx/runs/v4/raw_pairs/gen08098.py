from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.product_name"), F.col("b.category"))
)
