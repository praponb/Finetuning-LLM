from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .subtract(df_orders.filter(F.year("order_date") >= 2023).select("amount"))
)
