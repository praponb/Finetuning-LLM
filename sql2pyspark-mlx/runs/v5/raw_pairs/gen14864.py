from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 4)
    .select("unit_price")
    .unionByName(df_transactions.select(F.col("amount").alias("unit_price")))
)
