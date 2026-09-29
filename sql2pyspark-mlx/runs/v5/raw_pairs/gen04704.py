from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("signup_date")
    .intersect(df_orders.filter(F.col("discount") >= 0.27).select("order_date"))
)
