from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2025-06-01")
    .select("status")
    .intersect(df_payments.select("status"))
)
