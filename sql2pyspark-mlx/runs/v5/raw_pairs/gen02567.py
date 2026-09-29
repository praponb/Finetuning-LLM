from pyspark.sql import functions as F

avg_amount = df_orders.filter(F.col("customer_id") == 35).agg(F.avg("amount")).first()[0]

result = (
    df_orders
    .filter(F.col("amount") >= avg_amount)
    .select("id", "amount", "discount", "customer_id")
)
