from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .subtract(df_order_items.select("unit_price"))
    .orderBy(F.col("shipping_fee").desc())
)
