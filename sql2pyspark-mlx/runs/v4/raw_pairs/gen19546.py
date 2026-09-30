from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .intersect(df_orders.select("shipping_fee"))
)
