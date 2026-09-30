from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 27)
    .select("customer_id")
    .intersect(df_orders.filter(F.col("store_id") == 9).select("customer_id"))
    .orderBy("customer_id")
)
