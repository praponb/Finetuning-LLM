from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("channel", "store_id").orderBy("amount", "id")

result = (
    df_orders
    .withColumn("amount_bucket", F.ntile(10).over(w))
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 1).over(w2))
)
