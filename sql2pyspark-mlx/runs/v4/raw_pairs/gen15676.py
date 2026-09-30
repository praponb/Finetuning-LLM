from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id", "channel").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("channel", "status")

result = (
    df_orders
    .withColumn("amount_bucket", F.ntile(3).over(w_ordered))
    .withColumn("corr_amount_shipping_fee", F.corr("amount", "shipping_fee").over(w2))
)
