from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") > 0.29) & (F.col("order_date") > "2024-04-01"))
    .select("id", "order_date", "shipping_fee")
)
