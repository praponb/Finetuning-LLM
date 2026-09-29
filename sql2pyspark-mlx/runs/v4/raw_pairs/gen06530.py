from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_orders
    .withColumn("shipping_fee_bucket", F.ntile(10).over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)
