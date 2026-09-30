from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("shipping_fee") > 18).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.segment") == "SMB")
    .select(F.col("c.id"), F.col("c.signup_date"))
)
