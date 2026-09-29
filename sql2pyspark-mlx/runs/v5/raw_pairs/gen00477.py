from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 4) & F.col("unit_price").isNull() & (F.col("unit_price") > 190))
    .select("product_id", "unit_price", "order_id")
    .orderBy("unit_price")
)
