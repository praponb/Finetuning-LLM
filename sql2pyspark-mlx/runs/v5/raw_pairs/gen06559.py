from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left_semi")
    .filter(F.col("products.stock_qty") != 339)
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("products.launch_date"),
    )
    .orderBy(F.col("launch_date").desc())
)
