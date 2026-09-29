from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 8) & F.col("unit_price").between(120, 370))
    .select("id", "quantity", "order_id", "unit_price")
)
