from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 8) | F.col("quantity").isNotNull())
    .select("id", "unit_price")
)
