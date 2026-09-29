from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 220) & (F.col("unit_price") <= 180))
    .select("product_id")
)
