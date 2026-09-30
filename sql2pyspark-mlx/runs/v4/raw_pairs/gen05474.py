from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") < "2023-06-01")
    .select("channel")
    .distinct()
)
