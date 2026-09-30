from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(230, 400) & (F.col("quantity") == 6) & (F.col("quantity") != 5))
    .withColumn("quantity_whole", F.floor("quantity"))
)
