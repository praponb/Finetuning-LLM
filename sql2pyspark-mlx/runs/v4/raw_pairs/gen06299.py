from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.launch_date")), "left_anti")
)
