from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("order_id", "quantity", "unit_price")
)
