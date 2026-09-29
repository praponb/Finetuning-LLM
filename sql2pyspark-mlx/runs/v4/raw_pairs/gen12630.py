from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(900, 1530) & F.col("channel").isNotNull())
    .select("id", "store_id", "order_date")
)
