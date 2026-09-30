from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") <= 38) & F.col("customer_id").isNull() & (F.col("discount") <= 0.3))
    .select("id", "discount", "order_date")
)
