from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .select(
        F.col("oi.product_id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
    .orderBy("category")
)
