from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 8))
    .withColumn("unit_price_whole", F.ceil("unit_price"))
)
