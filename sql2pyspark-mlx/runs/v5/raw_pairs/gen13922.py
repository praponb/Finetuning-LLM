from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", F.lit("customer").alias("origin"))
    .union(df_orders.select("order_date", F.lit("order").alias("origin")))
    .distinct()
    .orderBy("signup_date")
)
