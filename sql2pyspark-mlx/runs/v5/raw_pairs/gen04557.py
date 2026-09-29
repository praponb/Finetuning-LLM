from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .unionByName(df_orders.select(F.col("order_date").alias("signup_date")))
    .distinct()
    .orderBy("signup_date")
)
