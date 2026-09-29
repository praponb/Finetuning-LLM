from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc())
w2 = Window.partitionBy("customer_id").orderBy("product_id")
w3 = Window.partitionBy("customer_id")

exploded = df_carts.select("id", "customer_id", F.inline("items"))

result = (
    exploded
    .withColumn("qty_percent_rank", F.percent_rank().over(w))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w2))
    .withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w3)), 4))
)
