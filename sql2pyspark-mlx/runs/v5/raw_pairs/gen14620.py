from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "unit_price", "order_id", "product_id")
    .orderBy("product_id")
)
