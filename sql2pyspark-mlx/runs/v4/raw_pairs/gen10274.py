from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") <= "2025-04-01")
    .select("customer_id")
    .subtract(df_orders.filter(F.col("shipping_fee") <= 24).select("customer_id"))
    .orderBy("customer_id")
)
