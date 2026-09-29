from pyspark.sql import functions as F

min_order_date = df_orders.filter(F.col("shipping_fee").between(14, 17)).agg(F.min("order_date")).first()[0]

result = (
    df_orders
    .filter(F.col("order_date") <= min_order_date)
    .select("id", "order_date", "status", "store_id")
    .orderBy("order_date")
)
