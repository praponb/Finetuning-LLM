from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("store_id").isin([9, 24, 33])).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
    .filter(F.year(F.col("c.signup_date")) >= 2023)
    .select(F.col("c.id"), F.col("c.age"), F.col("c.signup_date"))
    .orderBy("age", "id")
    .limit(5)
)
