from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2025-10-01")
    .select("store_id")
)
