from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .intersect(df_orders.select("order_date"))
    .orderBy("launch_date")
)
