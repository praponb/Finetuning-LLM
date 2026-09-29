from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 15, 23, 34]))
    .select(F.col("unit_price").alias("value"))
    .unionByName(df_transactions.select(F.col("amount").alias("value")))
    .distinct()
    .orderBy(F.col("value").desc())
)
