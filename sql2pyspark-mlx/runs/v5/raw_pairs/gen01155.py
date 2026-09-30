from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .subtract(df_orders.filter(F.col("order_date") < "2024-10-01").select("amount"))
)
