from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2023-03-01")
    .select("opened_date")
    .subtract(df_orders.filter(F.col("customer_id") == 4).select("order_date"))
    .orderBy("opened_date")
)
