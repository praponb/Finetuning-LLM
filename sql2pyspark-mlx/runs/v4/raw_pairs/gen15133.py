from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([12, 18, 32, 33]) | F.month("order_date").isin([3, 6, 9]))
    .select("id", "status", "amount", "customer_id")
)
