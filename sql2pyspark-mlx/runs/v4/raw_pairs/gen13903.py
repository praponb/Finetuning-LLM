from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_last())

base = (
    df_order_items
    .filter(F.col("quantity").isNull())
    .select(
        "*",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

result = (
    base
    .withColumn("prev_quantity", F.lag("quantity", 2).over(w))
    .withColumn("prev_unit_price", F.lag("unit_price", 2).over(w2))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w3))
)
