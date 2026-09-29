from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([12, 25, 26]))
    .select("unit_price")
    .intersect(df_orders.filter(F.col("amount") < 1290).select("amount"))
)
