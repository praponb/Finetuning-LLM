from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .withColumn("quantity_percent_rank", F.percent_rank().over(w))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w2))
)
