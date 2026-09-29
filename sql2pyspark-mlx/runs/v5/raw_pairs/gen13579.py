from pyspark.sql import functions as F

min_quantity = df_order_items.agg(F.min("quantity")).first()[0]

result = (
    df_order_items
    .filter(F.col("quantity") == min_quantity)
    .select("id", "quantity", "unit_price")
)
