from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") >= 0.32)
    .select("amount", "customer_id")
    .orderBy("customer_id")
)
