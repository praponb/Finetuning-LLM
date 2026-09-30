from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([22, 24, 36]))
    .select("product_id")
    .intersect(df_order_items.filter(F.col("unit_price") <= 120).select("product_id"))
    .orderBy("product_id")
)
