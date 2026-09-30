from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("order_id").isin([14, 24])).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.launch_date"),
    )
)
