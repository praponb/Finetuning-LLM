from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .select(
        F.col("products.stock_qty"),
        F.col("order_items.product_id"),
        F.col("order_items.order_id"),
        F.col("order_items.quantity"),
        F.last_day(F.col("products.launch_date")).alias("launch_date_month_end"),
    )
    .orderBy(F.col("product_id").desc())
)
