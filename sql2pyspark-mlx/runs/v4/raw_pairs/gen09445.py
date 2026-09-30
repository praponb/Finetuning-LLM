from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 6) & ((F.col("product_id") == 26) | F.col("order_id").isin([1, 21, 25])))
    .select("quantity", "order_id")
)
