from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
    .withColumn("quantity_whole", F.floor("quantity"))
)
