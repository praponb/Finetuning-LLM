from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .union(df_products.select("price"))
    .distinct()
)
