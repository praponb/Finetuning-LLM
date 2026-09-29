from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 8]))
    .select("product_id")
    .subtract(df_order_items.filter(F.col("order_id") == 17).select("product_id"))
)
