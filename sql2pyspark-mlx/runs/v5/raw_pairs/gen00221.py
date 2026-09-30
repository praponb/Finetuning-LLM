from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .subtract(df_customers.select(F.col("signup_date").alias("order_date")))
)
