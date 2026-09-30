from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 7) | F.col("product_id").isNull())
    .select("id", "order_id")
)
