from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.price"),
        F.coalesce(F.col("i.quantity"), F.lit(0)).alias("quantity_filled"),
    )
)
