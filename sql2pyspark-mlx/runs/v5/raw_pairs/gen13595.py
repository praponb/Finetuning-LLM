from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .withColumn("unit_price_bucket", F.ntile(10).over(w))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4))
)
