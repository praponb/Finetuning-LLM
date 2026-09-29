from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("quantity_whole", F.floor("quantity"))
    .orderBy(F.col("product_id").desc(), "id")
    .limit(1)
)
