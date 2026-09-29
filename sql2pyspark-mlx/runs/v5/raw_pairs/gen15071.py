from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount")
    .subtract(df_order_items.filter(F.col("order_id") == 12).select("unit_price"))
)
