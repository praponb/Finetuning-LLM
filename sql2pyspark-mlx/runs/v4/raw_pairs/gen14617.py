from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") == 2025) | F.col("shipping_fee").between(10, 21))
    .select("status", "channel")
)
