from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .union(df_orders.filter(F.col("discount") <= 0.29).select("order_date"))
    .distinct()
    .orderBy("sale_date")
)
