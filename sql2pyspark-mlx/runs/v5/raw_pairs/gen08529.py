from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.upper(F.trim("email")) == "IVY@GMAIL.COM")
    .select("*", F.length("name").alias("name_length"))
)

result = (
    base
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.signup_date")), "left_anti")
)
