from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "PENDING") | (F.col("status") != "RETURNED"))
    .select("id", "store_id", "order_date")
)
