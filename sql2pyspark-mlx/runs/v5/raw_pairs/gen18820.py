from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.col("products.price").between(320, 900) & (F.col("products.supplier_id") == 12))
    .select(
        F.col("products.cost"),
        F.col("inventory.store_id"),
        F.dayofmonth(F.col("products.launch_date")).alias("launch_date_day"),
    )
)
