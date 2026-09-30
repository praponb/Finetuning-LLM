from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 24) & (F.col("unit_price") > 350))
    .select("id", "product_id", "order_id", "quantity")
)
