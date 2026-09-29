from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([6, 13, 29])), "left_anti")
)
