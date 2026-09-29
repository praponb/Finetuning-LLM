from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
)
