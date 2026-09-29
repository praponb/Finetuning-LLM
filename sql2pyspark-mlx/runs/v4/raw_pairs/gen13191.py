from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("greatest_quantity_unit_price").asc_nulls_last())

cleaned = (
    df_order_items
    .filter(F.col("product_id") == 9)
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

result = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
