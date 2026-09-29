from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("status")

result = (
    df_orders
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 2).over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)
