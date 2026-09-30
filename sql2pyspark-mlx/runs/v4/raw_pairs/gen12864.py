from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .subtract(df_products.select("price"))
    .orderBy("shipping_fee")
)
