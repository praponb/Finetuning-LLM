from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("id", "customer_id", "amount", "store_id")
)
