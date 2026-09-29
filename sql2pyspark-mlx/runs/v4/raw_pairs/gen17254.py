from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.launch_date")), "left_anti")
)
