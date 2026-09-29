from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "product_id")
    .orderBy("id", "product_id")
    .limit(10)
)
