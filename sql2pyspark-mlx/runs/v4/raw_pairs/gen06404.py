from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("quantity") > 75).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.launch_date"),
    )
    .orderBy("stock_qty", "brand")
)
