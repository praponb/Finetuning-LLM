from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 9, 18, 19]))
    .select("id", "unit_price", "product_id", "quantity")
    .orderBy("id")
    .limit(5)
)
