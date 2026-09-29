from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .union(df_orders.select("order_date"))
    .distinct()
    .orderBy("sale_date")
)
