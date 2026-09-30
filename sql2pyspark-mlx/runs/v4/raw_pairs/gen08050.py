from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 360) & F.col("order_id").isin([13, 32, 38]))
    .select("id", "quantity")
)
