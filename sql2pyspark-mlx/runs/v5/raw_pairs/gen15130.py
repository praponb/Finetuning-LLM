from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select("id", "shipping_fee", "order_date", "status")
    .orderBy("order_date")
)
